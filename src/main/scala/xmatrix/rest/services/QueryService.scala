package xmatrix.rest.services

/**
  * Created by iodone on {18-5-11}.
  */


import java.util.UUID

import org.apache.log4j.Logger

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.mutable.{ListBuffer, Map}
import scala.collection.immutable.{Map => IMap}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import xmatrix.core.job._
import xmatrix.core.platform.SparkRuntime
import xmatrix.dsl.{ScriptSQLExec, ScriptSQLExecListener}
import xmatrix.rest.entities._
import xmatrix.rest.serializers.JsonSupport
import xmatrix.common.ErrorMsgHandler._

import scala.collection.JavaConversions._


trait QueryService extends JsonSupport with HelperService with BaseService{
  val logger = Logger.getLogger(classOf[QueryService])

  def querySql(sqlQuery: SqlQuery): Response = {
    val sparkRuntime = runtime.asInstanceOf[SparkRuntime]
    val sparkSession = sparkRuntime.sparkSession
    val jobInfo = SparkJobManager.buildSparkJobInfo(sqlQuery.owner, SparkJobType.SQL, sqlQuery.jobName, sqlQuery.sql, "",SparkJobState.SUBMITTED, sparkSession.sparkContext.applicationId, sqlQuery.timeout)

    sqlQuery.async match {
      case "true" => Response(Map("reqId" -> UUID.randomUUID(), "statusCode" -> 404, "errCode" -> "0", "errMsg" -> "Not "), "[]")
      case "false" =>
        def operator():Response = {
          var res = ""
          var meta = Map("reqId" -> UUID.randomUUID(), "statusCode" -> 200, "errCode" -> "0", "errMsg" -> "")
          try {
            res = sparkSession.sql(sqlQuery.sql).toJSON.collect().mkString(",")
          } catch {
            case e: Exception =>
              e.printStackTrace()
              val errMsg = e.getStackTrace.map(m => m.toString).mkString("\n")
              meta("statusCode") = 400
              meta("errMsg") = errMsg
              meta("errCode") = 9001001
          }

          val data = parse("[" + res + "]", false)
          Response(meta, data)
        }

        SparkJobManager.run[Response](sparkSession, jobInfo, operator)
    }
  }

  def queryXqlV2(xqlQuery: XqlQuery): Response = {
    logger.info("using queryXQL_V2")
    logger.info(s"Receive xql request: {owner:${xqlQuery.owner}, jobName:${xqlQuery.jobName}, groupId:${xqlQuery.groupId}, sql:${xqlQuery.sql}, checkpoint:${xqlQuery.checkpoint}")
    val sparkRuntime = runtime.asInstanceOf[SparkRuntime]
    val sparkSession = sparkRuntime.sparkSession
    val jobType = sparkRuntime.params.getOrDefault("xmatrix.type", SparkJobType.SCRIPT).toString

    val meta = Map("reqId" -> UUID.randomUUID().toString, "statusCode" -> 200, "errCode" -> "0", "errMsg" -> "")

    val jobInfo = SparkJobManager.buildSparkJobInfo(xqlQuery.owner, jobType, xqlQuery.jobName, xqlQuery.sql, xqlQuery.groupId, SparkJobState.SUBMITTED, sparkSession.sparkContext.applicationId,xqlQuery.timeout)
    SparkJobManager.setJobInfo(jobInfo.groupId, jobInfo)
    sparkSession.sparkContext.setLocalProperty("spark.scheduler.pool", xqlQuery.pool)
    val allPathPrefix = parse(xqlQuery.allPathPrefix, false).extractOrElse[IMap[String, String]](IMap())
    val scriptSQLExecListener = new ScriptSQLExecListener(sparkSession, xqlQuery.defaultPathPrefix, allPathPrefix, xqlQuery.groupId match { case "" => "" case _ => xqlQuery.groupId }, xqlQuery.checkpoint match { case Some(i) => i.toBoolean case None => false }, jobInfo.groupId)
    scriptSQLExecListener.addEnv("jobType", jobType)
    var info = Map[Any, Any]()

    try{
      xqlQuery.async.toBoolean match {
        case true =>
          Future {
            try{
              jobType match {
                case SparkJobType.STREAM =>
                  sparkSession.streams.active.filter { f => f.name == xqlQuery.jobName }.lastOption match {
                    case Some(i) =>
                      throw new RuntimeException(s"A stream job streamGroupId = ${xqlQuery.jobName} is already running.")
                    case None =>
                  }
                  SparkJobManager.run[Unit](sparkSession, jobInfo, f = () => {
                    logger.info(s"Receive stream job name: ${jobInfo.jobName}")
                    scriptSQLExecListener.addEnv("streamName", jobInfo.jobName)
                    scriptSQLExecListener.addEnv("owner", jobInfo.owner)
                    scriptSQLExecListener.addEnv("startingOffsets", xqlQuery.startingOffsets)
                    ScriptSQLExec.parse(xqlQuery.sql, scriptSQLExecListener)
                  })
                case SparkJobType.SCRIPT =>
                  SparkJobManager.run[Unit](sparkSession, jobInfo, f = () => {
                    ScriptSQLExec.parse(xqlQuery.sql, scriptSQLExecListener)
                    scriptSQLExecListener.dropTempTable()
                    val sucJobEvent = new JobEventInfo(jobInfo.groupId, -1, -1, -1, SparkJobState.SUCCEEDED, "", -1, System.currentTimeMillis(), SparkJobEventType.onJobGroupSuc)
                    sparkJobOperator.updateJobRuntimeState(sucJobEvent, IMap("jobType" -> jobType))
                  })
                case _ =>
              }
            } catch {
              case e: Throwable =>
                e.printStackTrace()
                val errMsg = e.getMessage match {
                  case null =>
                    e.getCause.toString
                  case _ =>
                    e.getMessage.split("\n")(0)
                }
                val failJobEvent = new JobEventInfo(jobInfo.groupId, -1, -1, -1, SparkJobState.FAILED, errHandle(errMsg), -1, System.currentTimeMillis(), SparkJobEventType.onJobGroupFail)
                sparkJobOperator.updateJobRuntimeState(failJobEvent, IMap("jobType" -> jobType))
            }
          } //end future

          Response(meta, info)

        case false =>
          sparkSession.sparkContext.setJobGroup(jobInfo.groupId, "", true)
            jobType match {
              case SparkJobType.STREAM =>
                throw new RuntimeException("Async must be true when submit a stream job.")
              case SparkJobType.SCRIPT =>
                var resultData = ""
                ScriptSQLExec.parse(xqlQuery.sql, scriptSQLExecListener)
                scriptSQLExecListener.dropTempTable()
                resultData = scriptSQLExecListener.getLastSelectTable() match {
                  case Some(table) =>
                    val sql = s"select * from $table limit 10"
                    sparkSession.sql(sql).show()
                    "[" + sparkSession.sql(s"select * from $table limit 100").toJSON.collect().mkString(",") + "]"
                  case None => "[]"
                }
                info("result") = resultData
                val sucJobEvent = new JobEventInfo(jobInfo.groupId, -1, -1, -1, SparkJobState.SUCCEEDED, "", -1, System.currentTimeMillis(), SparkJobEventType.onJobGroupSuc)
                sparkJobOperator.updateJobRuntimeState(sucJobEvent, IMap("jobType" -> jobType))
                Response(meta, info)
            }
      }
    } catch {
      case e: Throwable =>
        e.printStackTrace()
        val errMsg = e.getMessage match {
          case null =>
            e.getCause.toString
          case _ =>
            e.getMessage.split("\n")(0)
        }
        val failJobEvent = new JobEventInfo(jobInfo.groupId, -1, -1, -1, SparkJobState.FAILED, errHandle(errMsg), -1, System.currentTimeMillis(), SparkJobEventType.onJobGroupFail)
        sparkJobOperator.updateJobRuntimeState(failJobEvent, IMap("jobType" -> jobType))
        meta("errCode") = 9001001
        meta("statusCode") = 503
        meta("errMsg") = e.getMessage
        Response(meta, e.getMessage)
    }

  }

  def queryXql(xqlQuery: XqlQuery): Response = {
    logger.info(s"Receive xql request: {owner:${xqlQuery.owner}, jobName:${xqlQuery.jobName}, groupId:${xqlQuery.groupId}, sql:${xqlQuery.sql}, checkpoint:${xqlQuery.checkpoint}")
    val sparkRuntime = runtime.asInstanceOf[SparkRuntime]
    val sparkSession = sparkRuntime.sparkSession
    val jobType = sparkRuntime.params.getOrDefault("xmatrix.type", SparkJobType.SCRIPT).toString

    val meta = Map("reqId" -> UUID.randomUUID().toString, "statusCode" -> 200, "errCode" -> "0", "errMsg" -> "")

    try {
      jobType match  {
        case SparkJobType.STREAM =>
          sparkSession.streams.active.filter{f=>f.name == xqlQuery.jobName}.lastOption match  {
            case Some(i) =>
              throw new RuntimeException(s"A stream job streamGroupId = ${xqlQuery.jobName} is already running.")
            case None =>
          }
          if (xqlQuery.async.toBoolean == false)
            throw new RuntimeException("Async must be true when submit a stream job.")

        case SparkJobType.SCRIPT =>

        case _ =>
      }
    } catch {
      case e: Exception =>
        val errMsg = if (e.getMessage == null) {
          e.getCause
        } else {
          e.getMessage.split("\n").mkString(" ")
        }
        meta.update("statusCode", 503)
        meta.update("errCode", 1)
        meta.update("errMsg", errMsg)
        return Response(meta, IMap())
    }

    val jobInfo = SparkJobManager.buildSparkJobInfo(xqlQuery.owner, jobType, xqlQuery.jobName, xqlQuery.sql, xqlQuery.groupId, SparkJobState.SUBMITTED, sparkSession.sparkContext.applicationId,xqlQuery.timeout)
    SparkJobManager.setJobInfo(jobInfo.groupId, jobInfo)

    xqlQuery.async match {
      case "true" =>
        val respF = Future {
          SparkJobManager.run[Unit](sparkSession, jobInfo, f = () => {
            sparkSession.sparkContext.setLocalProperty("spark.scheduler.pool", xqlQuery.pool)
            val allPathPrefix = parse(xqlQuery.allPathPrefix, false).extractOrElse[IMap[String, String]](IMap())
            val defaultPathPrefix = xqlQuery.defaultPathPrefix
            try {
              logger.info(xqlQuery.checkpoint.getOrElse(false))
              val scriptSQLExecListener = new ScriptSQLExecListener(sparkSession, defaultPathPrefix, allPathPrefix, xqlQuery.groupId match { case "" => "" case _ => xqlQuery.groupId }, xqlQuery.checkpoint match { case Some(i) => i.toBoolean case None => false }, jobInfo.groupId)
              scriptSQLExecListener.addEnv("jobType", jobType)
              jobInfo.jobType match {
                case SparkJobType.STREAM =>
                    logger.info(s"Receive stream job name: ${jobInfo.jobName}")
                    scriptSQLExecListener.addEnv("streamName", jobInfo.jobName)
                    ScriptSQLExec.parse(xqlQuery.sql, scriptSQLExecListener)
                case SparkJobType.SCRIPT =>
                  ScriptSQLExec.parse(xqlQuery.sql, scriptSQLExecListener)
                  scriptSQLExecListener.dropTempTable()
                  val sucJobEvent = new JobEventInfo(jobInfo.groupId, -1, -1, -1, SparkJobState.SUCCEEDED, "", -1, System.currentTimeMillis(), SparkJobEventType.onJobGroupSuc)
                  sparkJobOperator.updateJobRuntimeState(sucJobEvent, IMap("jobType" -> jobType))
                case _ =>
              }
            } catch {
              case e: Exception =>
                e.printStackTrace()
                logger.info("in service")
                val errMsg = e.getMessage() match {
                  case null => e.getCause.toString
                  case _ => e.getMessage.split("\n").mkString(" ")
                }
                val failJobEvent = new JobEventInfo(jobInfo.groupId, -1, -1, -1, SparkJobState.FAILED, errHandle(errMsg), -1, System.currentTimeMillis(), SparkJobEventType.onJobGroupFail)
                sparkJobOperator.updateJobRuntimeState(failJobEvent, IMap("jobType" -> jobType))
            }
          })
        }

        Response(meta, respF)

      case "false" =>
        SparkJobManager.run[Response](sparkSession, jobInfo, () => {
          val allPathPrefix = parse(xqlQuery.allPathPrefix, false).extractOrElse[IMap[String, String]](IMap())
          var info = Map[Any, Any]()
          var resultData = ""
          val defaultPathPrefix = xqlQuery.defaultPathPrefix
          try {
            val context = new ScriptSQLExecListener(sparkSession, defaultPathPrefix, allPathPrefix, xqlQuery.groupId match {case "" => "" case _ => xqlQuery.groupId}, false, jobInfo.groupId)
            context.addEnv("jobType", jobType)
            ScriptSQLExec.parse(xqlQuery.sql, context)
            context.dropTempTable()
            resultData = context.getLastSelectTable() match {
                case Some(table) =>
                  val sql = s"select * from $table limit 10"
                  sparkSession.sql(sql).show()
                  "[" + sparkSession.sql(s"select * from $table limit 100").toJSON.collect().mkString(",") + "]"
                case None => "[]"
            }
            info("result") = resultData
            val sucJobEvent = new JobEventInfo(jobInfo.groupId, -1, -1, -1, SparkJobState.SUCCEEDED, "", -1, System.currentTimeMillis(), SparkJobEventType.onJobGroupSuc)
            sparkJobOperator.updateJobRuntimeState(sucJobEvent, IMap("jobType" -> jobType))
          } catch {
            case e:Exception =>
              e.printStackTrace()
              val errMsg = e.getMessage() match {
                case null => e.getCause.toString
                case _ => e.getMessage.split("\n").mkString(" ") + e.getCause.toString
              }
              val failJobEvent = new JobEventInfo(jobInfo.groupId, -1, -1, -1, SparkJobState.FAILED, errMsg, -1, System.currentTimeMillis(), SparkJobEventType.onJobGroupFail)
              sparkJobOperator.updateJobRuntimeState(failJobEvent, IMap("jobType" -> jobType))
              meta("errCode") = 9001001
              meta("statusCode") = 503
              meta("errMsg") = errMsg
          } finally {
           info = SparkJobManager.getGroupJobInfo(jobInfo.groupId) match {
              case Some(i) =>
                Map(
                  "owner" -> i.owner,
                  "jobType" -> i.jobType,
                  "jobName" -> i.jobName,
                  "jobContent" -> i.jobContent,
                  "appId" -> i.appId,
                  "groupId" -> i.groupId,
                  "status" -> i.status,
                  "failReason" -> i.failReason,
                  "startTime" -> i.startTime,
                  "endTime" -> i.endTime,
                  "timeout" -> i.timeout,
                  "result" -> resultData,
                  "submitTime" -> i.submitTime,
                  "stages" -> i.stages.toMap,
                  "path" -> i.path
                )

              case None =>  Map()
            }
          }
          Response(meta, info)
        })
    }
  }

  def getRunningJobList(joblist: JobList) = {

    val meta = Map("reqId" -> UUID.randomUUID(), "statusCode" -> 200, "errCode" -> "0", "errMsg" -> "")
    val resultList = ListBuffer[Any]()

    joblist.groupIds foreach {
      job =>
        SparkJobManager.getGroupJobInfo(job) match {
          case Some(i) =>
            i.jobType match {
              case SparkJobType.SCRIPT =>
                resultList += (
                  IMap(
                    "owner" -> i.owner,
                    "jobType" -> i.jobType,
                    "jobName" -> i.jobName,
                    "jobContent" -> i.jobContent,
                    "appId" -> i.appId,
                    "groupId" -> i.groupId,
                    "status" -> i.status,
                    "failReason" -> i.failReason,
                    "startTime" -> i.startTime,
                    "endTime" -> i.endTime,
                    "timeout" -> i.timeout,
                    "submitTime" -> i.submitTime,
                    "stages" -> i.stages.toMap,
                    "path" -> i.path
                  )
                )
              case _ =>
            }
          case None => resultList += (IMap())
        }
    }
    Response(meta, resultList.toList)
  }

  def getRunningJob(job: Job): Response = {

    var meta = Map("reqId" -> UUID.randomUUID(), "statusCode" -> 200, "errCode" -> "0", "errMsg" -> "")
    try{
      val info = job.jobType match {
        case SparkJobType.STREAM =>
          SparkJobManager.getGroupJobInfo(job.groupId)
        case _ =>

          SparkJobManager.getGroupJobInfo(job.groupId) match {
            case Some(i) =>
              IMap(
                "owner" -> i.owner,
                "jobType" -> i.jobType,
                "jobName" -> i.jobName,
                "jobContent" -> i.jobContent,
                "appId" -> i.appId,
                "groupId" -> i.groupId,
                "status" -> i.status,
                "failReason" -> i.failReason,
                "startTime" -> i.startTime,
                "endTime" -> i.endTime,
                "timeout" -> i.timeout,
                "submitTime" -> i.submitTime,
                "stages" -> i.stages.toMap,
                "path" -> i.path
              )
            case None => IMap()
          }
      }
      Response(meta, info)
    } catch {
      case e:Exception=>
        e.printStackTrace()
        meta = Map("reqId" -> UUID.randomUUID(), "statusCode" -> 503, "errCode" -> "0", "errMsg" -> "not found")
        Response(meta, "")
    }
  }

  def killRunningJob(job: Job): Response = {
    var meta = Map("reqId" -> UUID.randomUUID(), "statusCode" -> 200, "errCode" -> "0", "errMsg" -> "")

    job.jobType.toString match {
      case SparkJobType.STREAM =>
        import org.apache.spark.sql.streaming.StreamingQuery
        var streamJob: StreamingQuery = null
        var errMsg: String = ""
        try {
          val jobGroup = SparkJobManager.getGroupJobInfo(job.groupId).get
          try {
            streamJob = runtime.asInstanceOf[SparkRuntime].sparkSession.streams.active.filter{f=>f.name == jobGroup.jobName}.last
            streamJob.stop()
            errMsg = streamJob.exception match {
              case Some(i) => i.toString()
              case None => ""
            }
          } catch {
            case e:NoSuchElementException =>
              logger.error(s"Unable to found stream job ${jobGroup.jobName} in sparkSession, jobInfo status is ${jobGroup.status}")
              e.printStackTrace()
          }
          val streamEndEventInfo = new JobEventInfo(
            jobGroup.groupId.toString,
            -1,
            -1,
            -1,
            SparkJobState.KILLED,
            errMsg,
            -1,
            System.currentTimeMillis(),
            SparkJobEventType.onStreamEnd
          )
          sparkJobOperator.updateJobRuntimeState(streamEndEventInfo, IMap("jobType" -> SparkJobManager._jobType))
        } catch {
          case e:Exception =>
            e.printStackTrace()
            meta = Map("reqId" -> UUID.randomUUID(), "statusCode" -> 503, "errCode" -> "00000503", "errMsg" -> s"${e.getMessage}")
        }
        Response(meta,Map())

      case SparkJobType.SCRIPT =>
        try{
          val killTime = System.currentTimeMillis()
          SparkJobManager.killJob(job.groupId)
          val jobEventInfo = new JobEventInfo(job.groupId, -1, -1, -1, SparkJobState.KILLED, "user kill", -1, killTime, SparkJobEventType.onJobGroupKill)
          sparkJobOperator.updateJobRuntimeState(jobEventInfo, IMap("jobType" -> job.jobType))
        } catch {
          case e: Exception =>
            meta = Map("reqId" -> UUID.randomUUID(), "statusCode" -> 503, "errCode" -> "00000503", "errMsg" -> s"${e.getMessage}")
        }
        Response(meta, Map())
    }
  }

  def testXql(async: String, owner: String, jobName: String, sql: String, timeout: Int, groupId: String, pool: String, defaultPathPrefix: String, callback: String, startingOffsets: String): Response = {
    queryXqlV2(new XqlQuery(async, owner, jobName, sql, timeout, groupId, pool, "{}", defaultPathPrefix, callback, Some("false"), startingOffsets))
  }

}
