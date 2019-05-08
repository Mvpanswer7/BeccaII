package xmatrix.core.job

/**
  * Created by iodone on {18-5-18}.
  */

import org.apache.log4j.Logger
import xmatrix.common.ErrorMsgHandler._

import scala.collection.JavaConversions._

class SparkJobOperator() {

  val logger = Logger.getLogger(getClass.getName)

  def updateJobRuntimeState(jobEventInfo: JobEventInfo, env: Map[String, String]) = {
    env.get("jobType").get  match {
      case SparkJobType.SCRIPT =>
        SparkJobManager._jobManager.jobGroupDetail.synchronized {
          val jobGroupInfo = SparkJobManager.getJobInfoByGroupId(jobEventInfo.groupId)
          jobEventInfo.eventType match {
            case SparkJobEventType.onJobGroupSuc =>
              jobGroupInfo.status match {
                case SparkJobState.RUNNING =>
                  logger.info(s" ${jobEventInfo.eventType}: Job ${jobGroupInfo.groupId} status change ${jobGroupInfo.status} => ${jobEventInfo.status}")
                  jobGroupInfo.status = jobEventInfo.status
                  jobGroupInfo.endTime = if(jobGroupInfo.stages.last._2.endTime == -1) {
                    jobEventInfo.endTime
                  } else {
                    jobGroupInfo.stages.last._2.endTime
                  }
                case _ =>
              }

            case SparkJobEventType.onJobGroupFail | SparkJobEventType.onJobGroupKill =>
              if(jobGroupInfo.status != SparkJobState.KILLED) {
                logger.info(s" ${jobEventInfo.eventType}: Job ${jobGroupInfo.groupId} status change ${jobGroupInfo.status} => ${jobEventInfo.status}")
                jobGroupInfo.status = jobEventInfo.status
              }
              jobGroupInfo.endTime = jobEventInfo.endTime
              if(jobGroupInfo.failReason.isEmpty && !jobEventInfo.failReason.isEmpty) {
                jobGroupInfo.failReason = errHandle(jobEventInfo.failReason)
              }

            case SparkJobEventType.onJobStart =>
              jobGroupInfo.status match {
                case SparkJobState.SUBMITTED =>
                  logger.info(s" ${jobEventInfo.eventType}: Job ${jobGroupInfo.groupId} status change ${jobGroupInfo.status} => ${jobEventInfo.status}")
                  jobGroupInfo.status = jobEventInfo.status
                case _ =>
              }
            case SparkJobEventType.onJobEnd =>
            case SparkJobEventType.onStageSubmit =>
              jobGroupInfo.stages.put(jobEventInfo.stageId, new StageInfo(jobEventInfo.stageId, jobEventInfo.status, jobEventInfo.startTime, -1, jobEventInfo.endTime, errHandle(jobEventInfo.failReason), env.getOrElse("numTasks", "0").toInt, -1, -1))
            case SparkJobEventType.onStageEnd =>
              val stage = jobGroupInfo.stages.getOrDefault(jobEventInfo.stageId, new StageInfo(0, "", 0,0,0,"",0, -1, -1))
              stage.stageId = jobEventInfo.stageId
              stage.status = jobEventInfo.status
              stage.submitTime = jobEventInfo.startTime
              stage.endTime = jobEventInfo.endTime
              if(!jobEventInfo.failReason.isEmpty) {
                stage.failureReason = errHandle(jobEventInfo.failReason)
                jobGroupInfo.failReason = errHandle(jobEventInfo.failReason)
              }

            case SparkJobEventType.onTaskStart =>
              if (jobGroupInfo.startTime == -1 && (jobGroupInfo.status == SparkJobState.SUBMITTED || jobGroupInfo.status == SparkJobState.QUEUED)) {
                jobGroupInfo.startTime = jobEventInfo.startTime
                logger.info(s" ${jobEventInfo.eventType}: Job ${jobGroupInfo.groupId} status change ${jobGroupInfo.status} => ${jobEventInfo.status}")
                jobGroupInfo.status = jobEventInfo.status
              } else if (jobGroupInfo.status == SparkJobState.KILLED) {
                SparkJobManager.killJob(jobGroupInfo.groupId)
              }
              val stage = jobGroupInfo.stages.get(jobEventInfo.stageId)
              if ((stage.status == SparkJobState.SUBMITTED || stage.status == SparkJobState.QUEUED) && stage.firstTaskId == -1) {
                stage.status = jobEventInfo.status
                stage.startTime = jobEventInfo.startTime
                stage.firstTaskId = jobEventInfo.taskId
              }
              jobGroupInfo.stages.put(jobEventInfo.stageId, stage)

            case SparkJobEventType.onTaskEnd =>
              val totalTasks = jobGroupInfo.stages.get(jobEventInfo.stageId).numTasks.toDouble
              val firstTaskId = jobGroupInfo.stages.get(jobEventInfo.stageId).firstTaskId.toDouble
              val process = ((jobEventInfo.taskId + 1 - firstTaskId)/totalTasks)*100 formatted("%.0f")
              val stage = jobGroupInfo.stages.get(jobEventInfo.stageId)
              if (stage.process != 100) {
                stage.process = if(process.toInt <= 100) process.toInt else 100
              }
              jobGroupInfo.stages.put(jobEventInfo.stageId, stage)

            case SparkJobEventType.onUpdatePath =>
              jobGroupInfo.path += env.getOrElse("path", "test")
            case _ =>
          }
          SparkJobManager.setJobInfo(jobEventInfo.groupId, jobGroupInfo)
        }
      case SparkJobType.STREAM =>
        SparkJobManager._jobManager.jobGroupDetail.synchronized{
          val jobGroupInfo = SparkJobManager.getJobInfoByGroupId(jobEventInfo.groupId)
          jobEventInfo.eventType match {
            case SparkJobEventType.onStreamStart =>
              logger.info(s" ${jobEventInfo.eventType}: Job ${jobGroupInfo.groupId} status change ${jobGroupInfo.status} => ${jobEventInfo.status}")
              jobGroupInfo.status = jobEventInfo.status
              jobGroupInfo.queryId = jobEventInfo.queryId
              jobGroupInfo.runId = jobEventInfo.runId
              jobGroupInfo.startTime = jobEventInfo.startTime
            case SparkJobEventType.onStreamProgress =>
              logger.info(s" ${jobEventInfo.eventType}: Job ${jobGroupInfo.groupId} status change ${jobGroupInfo.status} => ${jobEventInfo.status}")
              jobGroupInfo.status = jobEventInfo.status
            case SparkJobEventType.onStreamEnd =>
              if (jobGroupInfo.status != SparkJobState.KILLED) {
                logger.info(s" ${jobEventInfo.eventType}: Job ${jobGroupInfo.groupId} status change ${jobGroupInfo.status} => ${jobEventInfo.status}")
                jobGroupInfo.status = jobEventInfo.status
              }
              if(jobGroupInfo.failReason.isEmpty) {
                jobGroupInfo.failReason = errHandle(jobEventInfo.failReason)
              }
              jobGroupInfo.endTime = jobEventInfo.endTime
            case SparkJobEventType.onJobGroupFail =>
              logger.info(s" ${jobEventInfo.eventType}: Job ${jobGroupInfo.groupId} status change ${jobGroupInfo.status} => ${jobEventInfo.status}")
              if(jobGroupInfo.status != SparkJobState.KILLED)
                jobGroupInfo.status = jobEventInfo.status
              if(jobGroupInfo.failReason.isEmpty && !jobEventInfo.failReason.isEmpty) {
                jobGroupInfo.failReason =  errHandle(jobEventInfo.failReason)
              }
              jobGroupInfo.endTime = jobEventInfo.endTime
            case SparkJobEventType.onUpdatePath =>
              jobGroupInfo.path += env.get("path").get
            case SparkJobEventType.onStageEnd =>
              if(jobGroupInfo.failReason.isEmpty && !jobEventInfo.failReason.isEmpty) {
                jobGroupInfo.failReason = errHandle(jobEventInfo.failReason)
              }
            case _ =>
          }
          SparkJobManager.setJobInfo(jobEventInfo.groupId, jobGroupInfo)
        }
      case SparkJobType.SQL =>
      case _ =>
    }
  }

}

case class JobEventInfo (groupId: String, jobId: Int, stageId: Int, taskId: Int, status: String, failReason: String, startTime: Long, endTime: Long, eventType: String, queryId: String = "", runId: String = "")
