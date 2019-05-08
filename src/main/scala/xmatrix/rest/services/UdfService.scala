package xmatrix.rest.services

import java.lang.reflect.Modifier
import java.util.UUID

import scala.collection.mutable.Map
import xmatrix.common.JarUtil
import xmatrix.core.platform.SparkRuntime
import xmatrix.rest.entities._
import xmatrix.rest.serializers.JsonSupport

trait UdfService extends JsonSupport with BaseService{

  def registerUdf(udfRegister: UdfRegister): Response = {
    var meta = Map("reqId" -> UUID.randomUUID(), "statusCode" -> 200, "errCode" -> "0", "errMsg" -> "")

    val sparkSession = runtime.asInstanceOf[SparkRuntime].sparkSession
    sparkSession.sparkContext.addJar(udfRegister.path)
    try {
      val clzz = JarUtil.loadJar(udfRegister.path, udfRegister.className)
      clzz.getMethods.foreach { f =>
        try {
          if (Modifier.isStatic(f.getModifiers)) {
            f.invoke(null, sparkSession.udf)
          }
        } catch {
          case e: Exception =>
            println(s"${f.getName} missing", e)
            meta = Map("reqId" -> UUID.randomUUID(), "statusCode" -> 503, "errCode" -> "00000503", "errMsg" -> s"${f.getName} missing, ${e.getMessage}")
        }
      }

    } catch {
      case e: Exception =>
        meta = Map("reqId" -> UUID.randomUUID(), "statusCode" -> 503, "errCode" -> "00000503", "errMsg" -> e.getMessage)
    }
    Response(meta, Map())
  }
}
