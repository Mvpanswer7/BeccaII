package xmatrix.core.job

/**
  * Created by iodone on {18-5-10}.
  */

import java.util.concurrent.{ConcurrentHashMap, Executors, TimeUnit}
import java.util.{Properties, UUID}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import org.apache.log4j.Logger
import org.apache.spark.scheduler._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import xmatrix.dsl.ScriptSQLExec
import xmatrix.common.ErrorMsgHandler._

object SparkJobManager {
  val logger = Logger.getLogger(classOf[SparkJobManager])
  var _jobManager: SparkJobManager = _
  var _jobType: String = _

  def init(ss: SparkSession, jobType: String, initialDelay: Long = 30, checkTimeInterval: Long = 5) = {
    synchronized {
      if (_jobManager == null) {
        logger.info(s"JobCanceller Timer  started with initialDelay=${initialDelay} checkTimeInterval=${checkTimeInterval}")
        _jobManager = new SparkJobManager(ss, initialDelay, checkTimeInterval)
        val operator = new SparkJobOperator
        _jobType = jobType
        _jobType match {
          case SparkJobType.SCRIPT =>
            _jobManager.addJobListener(new SparkOperatorListener(operator))
          case SparkJobType.STREAM =>
            ss.streams.addListener(new SparkStructStreamingListener(ss, operator, ss.sparkContext.getLocalProperty("kafkaAddr")))
          case SparkJobType.SQL =>
          case _ =>
        }
        if (ss.sparkContext.getLocalProperty("xmatrix.jobManager.service").toString.toBoolean) {
          _jobManager.run
        }
      }
    }
  }

  def run[T](session: SparkSession, job: SparkJobInfo, f: () => T): T = {
    try {
      val jobDesc = s"[jobName: ${job.jobName} -- groupId: ${job.groupId}]"
      logger.info(jobDesc)
      if (_jobManager == null) {
        f()
      } else {
        session.sparkContext.setJobGroup(job.groupId, jobDesc, true)
        _jobManager.jobGroupDetail.put(job.groupId, job)
        f()
      }
    } finally {
      ScriptSQLExec.clear
      session.sparkContext.clearJobGroup()
    }
  }


  def buildSparkJobInfo(owner: String,
                        jobType: String,
                        jobName: String,
                        jobContent: String,
                        groupId: String,
                        status: String,
                        appId: String,
                        timeout: Long): SparkJobInfo = {
    val submitTime = System.currentTimeMillis()
    val jobGroupId = if ("".equals(groupId))
      UUID.randomUUID().toString
    else
      groupId
    _jobType = jobType
    logger.info(s"building jobInfo $jobGroupId, status $status")
    SparkJobInfo(owner, jobType, jobName, jobContent, appId, jobGroupId, status, "", submitTime, -1, -1, timeout, new ConcurrentHashMap[Any, StageInfo]())
  }

  def setJobInfo(jobGroupId: String, job: SparkJobInfo) =
    _jobManager.jobGroupDetail.put(jobGroupId, job)

  def getJobInfo: Map[String, SparkJobInfo] =
    _jobManager.jobGroupDetail.asScala.toMap

  def getJobInfoByGroupId(jobGroupId: String): SparkJobInfo =
    _jobManager.jobGroupDetail.get(jobGroupId)

  def getGroupJobInfo(groupId: String): Option[SparkJobInfo] =
    _jobManager.jobGroupDetail.asScala.toMap.get(groupId)

  def killJob(groupId: String): Unit =
    _jobManager.cancelJobGroup(groupId)

  def handleJobDone(groupId: String): Unit = {
    val job = getJobInfo(groupId)
    job.status match {
      case SparkJobState.SUCCEEDED | SparkJobState.FAILED | SparkJobState.KILLED =>
        _jobManager.jobGroupDetail.remove(groupId)
      case _ =>
    }
  }

  def getJobGrouIdByStageId(stageId: Int): String = {
    var res = ""
    _jobManager.jobGroupDetail.asScala.toMap foreach {
      f =>
        if(f._2.stages.asScala.toMap.contains(stageId))
          res = f._1
    }
    res
  }

  def getJobGroupInfoByQueryId(queryId: String, runId: String): SparkJobInfo =
    _jobManager.jobGroupDetail.asScala.toMap.values.filter{ f=>f.queryId == queryId && f.runId == runId}.last

  def getStreamJobGroupInfo(name: String, queryId: String = "", runId: String = ""): SparkJobInfo =
    _jobManager.jobGroupDetail.asScala.toMap.values.filter{ f=>f.jobName == name && f.queryId == queryId && f.runId == runId}.last
}

class SparkOperatorListener(operator: SparkJobOperator) extends SparkListener {
  private val logger = Logger.getLogger(getClass.getName)

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    val groupId = jobStart.properties.getProperty("spark.jobGroup.id")
    val jobStartEventInfo = new JobEventInfo(groupId, jobStart.jobId, -1, -1, SparkJobState.QUEUED, "", jobStart.time, -1, SparkJobEventType.onJobStart)
    operator.updateJobRuntimeState(jobStartEventInfo, Map("jobType" -> SparkJobManager._jobType))
  }
  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {}

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    val groupId = stageSubmitted.properties.getProperty("spark.jobGroup.id")
    val stageSubmitEvent = new JobEventInfo(groupId, -1, stageSubmitted.stageInfo.stageId, -1, SparkJobState.SUBMITTED, "", stageSubmitted.stageInfo.submissionTime.get, -1, SparkJobEventType.onStageSubmit)
    operator.updateJobRuntimeState(stageSubmitEvent, Map("jobType" -> SparkJobManager._jobType, "numTasks" -> stageSubmitted.stageInfo.numTasks.toString ))
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val groupId = SparkJobManager.getJobGrouIdByStageId(stageCompleted.stageInfo.stageId)
    val stageEndEvent = new JobEventInfo(
      groupId,
      -1,
      stageCompleted.stageInfo.stageId,
      -1,
      stageCompleted.stageInfo.failureReason match {
        case Some(i) =>
          SparkJobState.FAILED
        case None =>
          SparkJobState.SUCCEEDED
      },
      stageCompleted.stageInfo.failureReason match {
        case null =>
          ""
        case Some(i) =>
          errHandle(i)
        case None => ""
      },
      stageCompleted.stageInfo.submissionTime.get,
      stageCompleted.stageInfo.completionTime.get,
      SparkJobEventType.onStageEnd)
    operator.updateJobRuntimeState(stageEndEvent, Map("jobType" -> SparkJobManager._jobType))
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    val groupId = SparkJobManager.getJobGrouIdByStageId(taskStart.stageId)
    val taskStartEvent = new JobEventInfo(groupId, -1, taskStart.stageId, taskStart.taskInfo.taskId.asInstanceOf[Int], SparkJobState.RUNNING, "", taskStart.taskInfo.launchTime, -1, SparkJobEventType.onTaskStart)
    operator.updateJobRuntimeState(taskStartEvent, Map("jobType" -> SparkJobManager._jobType))
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val groupId = SparkJobManager.getJobGrouIdByStageId(taskEnd.stageId)
    val taskEndEvent = new JobEventInfo(groupId, -1, taskEnd.stageId, taskEnd.taskInfo.taskId.toInt, taskEnd.taskInfo.status, taskEnd.reason.toString, taskEnd.taskInfo.launchTime, taskEnd.taskInfo.finishTime, SparkJobEventType.onTaskEnd)
    operator.updateJobRuntimeState(taskEndEvent, Map("jobType" -> SparkJobManager._jobType))
  }

}

class SparkStructStreamingListener(ss: SparkSession, operator: SparkJobOperator, bootstrpServer:String) extends StreamingQueryListener {
  override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
    val jobGroup = SparkJobManager.getStreamJobGroupInfo(ss.streams.get(queryStarted.id).name)
    val streamStartEventInfo = new JobEventInfo(jobGroup.groupId, -1, -1,-1, SparkJobState.RUNNING, "", System.currentTimeMillis(), -1, SparkJobEventType.onStreamStart, queryStarted.id.toString, queryStarted.runId.toString)
    operator.updateJobRuntimeState(streamStartEventInfo, Map("jobType" -> SparkJobManager._jobType))
  }
  override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
    val streamProgress = ss.streams.get(queryProgress.progress.id)
    val jobGroup = SparkJobManager.getStreamJobGroupInfo(streamProgress.name, streamProgress.id.toString, streamProgress.runId.toString)
    val streamProgressEventInfo = new JobEventInfo(jobGroup.groupId, -1, -1, -1, SparkJobState.RUNNING, "", -1, -1, SparkJobEventType.onStreamProgress, queryProgress.progress.id.toString)
    operator.updateJobRuntimeState(streamProgressEventInfo, Map("jobType" -> SparkJobManager._jobType))

    if ( bootstrpServer != null &&  !bootstrpServer.isEmpty) {
      val props = new Properties()
      props.put("bootstrap.servers", bootstrpServer)
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("batch.size", "0")

      val producer = new KafkaProducer[String,String](props)
      producer.send(new ProducerRecord[String, String]("xmatrix_ss_metrics",queryProgress.progress.json))
      producer.flush()
      producer.close()
    }
  }
  override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
    val jobGroup = SparkJobManager.getJobGroupInfoByQueryId(queryTerminated.id.toString, queryTerminated.runId.toString)
    val streamEndEventInfo = new JobEventInfo(jobGroup.groupId, -1, -1, -1, SparkJobState.FAILED, queryTerminated.exception.toString, -1, System.currentTimeMillis(), SparkJobEventType.onStreamEnd)
    operator.updateJobRuntimeState(streamEndEventInfo, Map("jobType" -> SparkJobManager._jobType))
  }
}

class SparkJobManager(ss: SparkSession, initialDelay: Long, checkTimeInterval: Long) {
  val jobGroupDetail = new ConcurrentHashMap[String, SparkJobInfo]()
  val logger = Logger.getLogger(classOf[SparkJobManager])
  val executor = Executors.newSingleThreadScheduledExecutor()

  def run = {
    executor.scheduleWithFixedDelay(new Runnable {
      override def run(): Unit = {
        jobGroupDetail.filter(f=>f._2.status == SparkJobState.RUNNING).foreach { f =>
          val elapseTime = System.currentTimeMillis() - f._2.startTime
          if (f._2.timeout > 0 && elapseTime >= f._2.timeout) {
            logger.info("Timeout reached. JobCanceller Timer cancel job group " + f._1)
            cancelJobGroup(f._1)
          }
        }
      }
    }, initialDelay, checkTimeInterval, TimeUnit.SECONDS)
  }

  def cancelJobGroup(groupId: String): Unit =
    ss.sparkContext.cancelJobGroup(groupId)

  def addJobListener(listener: SparkListener): Unit =
    ss.sparkContext.addSparkListener(listener)
}

case object SparkJobType {
  val SCRIPT = "script"
  val SQL = "sql"
  val STREAM = "stream"
}

case object SparkJobState {
  val SUBMITTED = "SUBMITTED"
  val QUEUED = "QUEUED"
  val RUNNING = "RUNNING"
  val SUCCEEDED = "SUCCEEDED"
  val FAILED = "FAILED"
  val KILLED = "KILLED"
  val LOST = "LOST"
}

case object SparkJobEventType {
  val onJobGroupSuc = "onJobGroupSuc"
  val onJobGroupFail = "onJobGroupFail"
  val onJobGroupKill = "onJobGroupKill"
  val onJobStart = "onJobStart"
  val onJobEnd = "onJobEnd"
  val onUpdatePath = "onUpdatePath"
  val onStageSubmit = "onStageSubmit"
  val onStageEnd = "onStageEnd"
  val onTaskStart = "onTaskStart"
  val onTaskEnd = "onTaskEnd"
  val onStreamStart = "onStreamStart"
  val onStreamProgress = "onStreamProgress"
  val onStreamEnd = "onStreamEnd"
}

case class SparkJobInfo(
                         owner: String,
                         jobType: String,
                         var jobName: String,
                         jobContent: String,
                         appId: String,
                         groupId: String,
                         var status: String,
                         var failReason: String,
                         val submitTime: Long = -1,
                         var startTime: Long = -1,
                         var endTime: Long = -1,
                         timeout: Long = -1,
                         stages : ConcurrentHashMap[Any, StageInfo] = new ConcurrentHashMap[Any, StageInfo](),
                         var queryId: String = "",
                         var path : Set[String] = Set(),
                         var runId: String = ""
                       )

case class StageInfo( var stageId: Int, var status: String,  var submitTime: Long, var startTime: Long, var endTime: Long, var failureReason: String, var numTasks: Int, var firstTaskId: Long, var process: Int)

