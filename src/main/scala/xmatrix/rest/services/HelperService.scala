package xmatrix.rest.services

import java.util.UUID

import scala.collection.mutable.Map
import xmatrix.core.job._
import xmatrix.dsl.{ScriptSQLExec}
import xmatrix.rest.entities._
import xmatrix.rest.serializers.JsonSupport

trait HelperService extends JsonSupport with BaseService{

  def checkSyntax(xqlStr: String): Response = {
    var meta = Map("reqId" -> UUID.randomUUID(), "statusCode" -> 200, "errCode" -> "0", "errMsg" -> "")

    try{
      ScriptSQLExec.syntaxCheck(xqlStr)
    } catch {
      case e: Exception =>
        meta = Map("reqId" -> UUID.randomUUID(), "statusCode" -> 503, "errCode" -> "00000503", "errMsg" -> e.getMessage)
    }
    Response(meta, Map())
  }

  def removeJobInfo(job: Job): Response = {
    val meta = Map("reqId" -> UUID.randomUUID(), "statusCode" -> 200, "errCode" -> "0", "errMsg" -> "")
    SparkJobManager.handleJobDone(job.groupId)
    Response(meta, Map())
  }
}
