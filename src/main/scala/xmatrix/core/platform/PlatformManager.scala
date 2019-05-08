package xmatrix.core.platform

/**
  * Created by iodone on {18-5-10}.
  */

import java.lang.reflect.Modifier
import java.util.concurrent.atomic.AtomicReference
import java.util.{Map => JMap}

import akka.actor.ActorSystem

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.collection.immutable.{Map => IMap}
import org.apache.log4j.Logger
import xmatrix.common.{HDFSOperator, ParamsUtil}
import xmatrix.core.job._
import xmatrix.dsl.{ScriptSQLExec, ScriptSQLExecListener}
import xmatrix.rest.{RestApp, RestServer}




class PlatformManager {
  self =>
  val config = new AtomicReference[ParamsUtil]()
  val logger = Logger.getLogger(getClass.getName)

  val listeners = new ArrayBuffer[PlatformManagerListener]()

  def register(listener: PlatformManagerListener) = {
    listeners += listener
  }

  def unRegister(listener: PlatformManagerListener) = {
    listeners -= listener
  }

  def startRestServer(ip: String, port: String) = {
    RestApp.main(Map("ip"->ip, "port"->port))
  }

  def startThriftServer(runtime: XmatrixRuntime) = {
    runtime.startThriftServer
  }

//  def registerToZk(params: ParamsUtil) = {
//    zk = ZkRegister.registerToZk(params)
//  }
//
//  var zk: ZKClient = null


  def run(_params: ParamsUtil, reRun: Boolean = false) = {

    if (!reRun) {
      config.set(_params)
    }

    val params = config.get()

    val lastXmatrixRuntimeInfo = if (reRun) {
      val tempRuntime = PlatformManager.getRuntime
      tempRuntime.getClass.getMethod("clearLastInstantiatedContext").invoke(null)
      Some(tempRuntime.xmatrixRuntimeInfo)
    } else None


    val tempParams = new java.util.HashMap[Any, Any]()
    params.getParamsMap.filter(f => f._1.startsWith("xmatrix.")).foreach { f => tempParams.put(f._1, f._2) }
    val runtime = PlatformManager.getRuntime.asInstanceOf[SparkRuntime]


    lastXmatrixRuntimeInfo match {
      case Some(xsri) =>
        runtime.configureXmatrixRuntimeInfo(xsri)
        runtime.resetRuntimeOperator(null)
      case None =>
    }

    if (params.getBooleanParam("xmatrix.rest", false) && !reRun) {
      val ip = params.getParamAndCheck("xmatrix.rest.ip")
      val port = params.getParamAndCheck("xmatrix.rest.port")
      startRestServer(ip, port)
    }

    if (params.getBooleanParam("xmatrix.thrift", false)
      && !reRun
    ) {
      startThriftServer(runtime)
    }

//    if (params.hasParam("xmatrix.zk.conf_root_dir") && !reRun) {
//      registerToZk(params)
//    }


    if (params.getBooleanParam("xmatrix.unitest.startRuntime", true)) {
      runtime.startRuntime
    }

    if (params.getParam("xmatrix.udfClassPath", "") != "") {
      Class.forName(params.getParam("xmatrix.udfClassPath")).getMethods.foreach { f =>
        try {
          if (Modifier.isStatic(f.getModifiers)) {
            f.invoke(null, runtime.sparkSession.udf)
          }
        } catch {
          case e: Exception =>
            e.printStackTrace()
        }
      }
    }



    if (params.getParam("xmatrix.xql", "") != "") {
      val jobFilePath = params.getParam("xmatrix.xql")
      var xql = ""
      if (jobFilePath.startsWith("classpath://")) {
        val cleanJobFilePath = jobFilePath.substring("classpath://".length)
        xql = scala.io.Source.fromInputStream(
          PlatformManager.getClass.getResourceAsStream(cleanJobFilePath)).getLines().
          mkString("\n")
      } else {
        xql = HDFSOperator.readFile(jobFilePath)
      }

      val ss = runtime.asInstanceOf[SparkRuntime].sparkSession
      val jobType = params.getParam("xmatrix.type", SparkJobType.SCRIPT)
      val groupId = ss.sparkContext.appName
      val jobInfo = SparkJobManager.buildSparkJobInfo("xmatrix", jobType, "xmatrix", xql, groupId, SparkJobState.SUBMITTED, ss.sparkContext.applicationId, -1l)
      SparkJobManager.setJobInfo(jobInfo.groupId, jobInfo)
      val sparkJobOperator = new SparkJobOperator()

      SparkJobManager.run[Unit](ss, jobInfo, () => {
        var message = ""
        try {
          val context = new ScriptSQLExecListener(ss, "", Map[String, String](),"",true, groupId)
          if(jobInfo.jobType == SparkJobType.STREAM) {
            context.addEnv("streamName", jobInfo.groupId)
          }
          ScriptSQLExec.parse(xql, context)
          jobType match {
            case SparkJobType.SCRIPT =>
              val sucJobEvent = new JobEventInfo(jobInfo.groupId, -1, -1, -1, SparkJobState.SUCCEEDED, "", -1, -1, SparkJobEventType.onJobGroupSuc)
              sparkJobOperator.updateJobRuntimeState(sucJobEvent, IMap("jobType" -> jobType))
              val resultData = context.getLastSelectTable() match {
                case Some(table) =>
                  val sql = s"select * from $table limit 20"
                  ss.sql(sql).show()
                  "[" + ss.sql(s"select * from $table limit 100").toJSON.collect().mkString(",") + "]"
                case None => "[]"
              }
              logger.info("============= SUCCESS ===========")
              message = resultData
            case _ =>
          }
          message

        } catch {
          case e:Exception =>
            e.printStackTrace()
            message = "========== FAILED =========="
            throw new RuntimeException("Spark Job Failed.")
        } finally {
          logger.info(message)
          ss.stop()
          System.exit(0)
        }
      })

    }

    if (params.getBooleanParam("xmatrix.spark.service", false)) {
      runtime.awaitTermination
    }

  }

  PlatformManager.setLastInstantiatedContext(self)
}

object PlatformManager {
  private val INSTANTIATION_LOCK = new Object()

  /**
    * Reference to the last created SQLContext.
    */
  @transient private val lastInstantiatedContext = new AtomicReference[PlatformManager]()

  /**
    * Get the singleton SQLContext if it exists or create a new one using the given SparkContext.
    * This function can be used to create a singleton SQLContext object that can be shared across
    * the JVM.
    */
  def getOrCreate: PlatformManager = {
    INSTANTIATION_LOCK.synchronized {
      if (lastInstantiatedContext.get() == null) {
        new PlatformManager()
      }
    }
    lastInstantiatedContext.get()
  }

  private[platform] def clearLastInstantiatedContext(): Unit = {
    INSTANTIATION_LOCK.synchronized {
      lastInstantiatedContext.set(null)
    }
  }

  private[platform] def setLastInstantiatedContext(platformManager: PlatformManager): Unit = {
    INSTANTIATION_LOCK.synchronized {
      lastInstantiatedContext.set(platformManager)
    }
  }


  def createRuntimeByPlatform(name: String, tempParams: java.util.Map[Any, Any]): XmatrixRuntime = {
    Class.forName(name).
      getMethod("getOrCreate", classOf[JMap[Any, Any]]).
      invoke(null, tempParams).asInstanceOf[XmatrixRuntime]
  }


  def clear = {
    lastInstantiatedContext.set(null)
  }


  def getRuntime: XmatrixRuntime = {
    val params: JMap[String, String] = getOrCreate.config.get().getParamsMap
    val tempParams: JMap[Any, Any] = params.map(f => (f._1.asInstanceOf[Any], f._2.asInstanceOf[Any]))

    val platformName = params.get("xmatrix.platform")
    val runtime = createRuntimeByPlatform(platformNameMapping(platformName), tempParams)

    runtime
  }


  def SPARK = "spark"


  def platformNameMapping = Map[String, String](
    SPARK -> "xmatrix.core.platform.SparkRuntime"
  )

}
