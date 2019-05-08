package xmatrix.core.platform

/**
  * Created by iodone on {18-5-10}.
  */

import java.lang.reflect.Modifier
import java.util.{Map => JMap}
import java.util.concurrent.atomic.AtomicReference

import org.apache.log4j.Logger

import scala.collection.JavaConversions._
import org.apache.spark.SparkConf
import org.apache.spark.ps.cluster.PSDriverBackend
import org.apache.spark.ps.local.LocalPSSchedulerBackend
import org.apache.spark.sql.{SparkSession, TiContext}
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2
import xmatrix.core.job.{SparkJobManager, SparkJobType, SparkRuntimeOperator}
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties




class SparkRuntime(_params: JMap[Any, Any]) extends XmatrixRuntime with PlatformManagerListener {
  self =>

  val logger = Logger.getLogger(getClass.getName)
  var tiContext: TiContext = _

  def name = "SPARK"

  var localSchedulerBackend: LocalPSSchedulerBackend = null
  var psDriverBackend: PSDriverBackend = null

  var sparkSession: SparkSession = createRuntime


  private var _xmatrixRuntimeInfo: SparkRuntimeInfo = new SparkRuntimeInfo(this)
  override def xmatrixRuntimeInfo = _xmatrixRuntimeInfo

  def createRuntime = {
    logger.info("create Runtime...")
    val conf = new SparkConf()
    params.filter(f =>
      f._1.toString.startsWith("spark.") ||
        f._1.toString.startsWith("hive.")
    ).foreach { f =>
      conf.set(f._1.toString, f._2.toString)
    }
    if (params.containsKey("xmatrix.master")) {
      conf.setMaster(params.get("xmatrix.master").toString)
    }

    conf.setAppName(params.get("xmatrix.name").toString)

    def isLocalMaster(conf: SparkConf): Boolean = {
      val master = conf.get("spark.master", "")
      master == "local" || master.startsWith("local[")
    }

    if (params.containsKey("xmatrix.ps.enable") && params.get("xmatrix.ps.enable").toString.toBoolean) {
      if (!isLocalMaster(conf)) {
        logger.info("register worker.sink.pservice.class with org.apache.spark.ps.cluster.PSServiceSink")
        conf.set("spark.metrics.conf.executor.sink.pservice.class", "org.apache.spark.ps.cluster.PSServiceSink")
      }
    }

    import org.apache.spark.sql.CarbonSession._

    var sparkSession: SparkSession.Builder = null

    sparkSession = SparkSession.builder().config(conf)

    if (params.containsKey("xmatrix.enableHiveSupport") &&
      params.get("xmatrix.enableHiveSupport").toString.toBoolean) {
      sparkSession.enableHiveSupport()
    }

    val ss = if (params.containsKey("xmatrix.enableCarbonDataSupport") &&
      params.get("xmatrix.enableCarbonDataSupport").toString.toBoolean) {

      CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy-MM-dd HH:mm:ss")
        .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy-MM-dd")

      val store = params.get("xmatrix.carbondata.store").toString
      val meta = params.get("xmatrix.carbondata.meta").toString

      if (params.containsKey("xmatrix.carbondata.warehouse")) {
        val warehouse = params.get("xmatrix.carbondata.warehouse").toString
        sparkSession.config("spark.sql.warehouse.dir", warehouse)
      }

      sparkSession.getOrCreateCarbonSession(store,meta)

    } else {
      sparkSession.getOrCreate()
    }

    val jobType = params("xmatrix.type") match {
      case SparkJobType.STREAM =>
        SparkJobType.STREAM
      case SparkJobType.SCRIPT =>
        SparkJobType.SCRIPT
      case SparkJobType.SQL =>
        SparkJobType.SQL
      case _ =>
        logger.info("xmatrix.type is wrong")
        ""
    }

    if (jobType == SparkJobType.STREAM && params.containsKey("xmatrix.metrics.kafka")) {
      val kafkaAddr = params.get("xmatrix.metrics.kafka").toString
      ss.sparkContext.setLocalProperty("kafkaAddr", kafkaAddr)
    }

      // init tiContext
    if (params.containsKey("xmatrix.tispark") && params.get("xmatrix.tispark").toString.toBoolean) {
      tiContext = new TiContext(ss)
    }

    if (params.containsKey("xmatrix.spark.service") && params.get("xmatrix.spark.service").toString.toBoolean) {
      ss.sparkContext.setLocalProperty("xmatrix.jobManager.service", "true")
    } else {
      ss.sparkContext.setLocalProperty("xmatrix.jobManager.service", "false")
    }

    SparkJobManager.init(ss, jobType)
    ss
  }

  params.put("_session_", sparkSession)

  registerUDF("xmatrix.core.udf.Functions")

  def registerUDF(clzz: String) = {
    logger.info("register functions..............")
    Class.forName(clzz).getMethods.foreach { f =>
      try {
        if (Modifier.isStatic(f.getModifiers)) {
          logger.info(f.getName)
          f.invoke(null, sparkSession.udf)
        }
      } catch {
        case e: Exception =>
          e.printStackTrace()
      }
    }

  }

  override def startRuntime: XmatrixRuntime= {
    this
  }

  override def awaitTermination: Unit = {
    Thread.currentThread().join()
  }


  override def destroyRuntime(stopGraceful: Boolean, stopContext: Boolean): Boolean = {
    sparkSession.stop()
    SparkRuntime.clearLastInstantiatedContext()
    true
  }


  override def configureXmatrixRuntimeInfo(xmatrixRuntimeInfo: XmatrixRuntimeInfo): Unit = {}

  override def resetRuntimeOperator(runtimeOperator: RuntimeOperator): Unit = {

  }

  override def params: JMap[Any, Any] = _params

  override def processEvent(event: Event): Unit = {}

  SparkRuntime.setLastInstantiatedContext(this)

  override def startThriftServer: Unit = {
    HiveThriftServer2.startWithContext(sparkSession.sqlContext)
  }

  override def startHttpServer: Unit = {}

}

class SparkRuntimeInfo(sr: SparkRuntime) extends XmatrixRuntimeInfo{
  var sparkOperator = new SparkRuntimeOperator(sr)
}

object SparkRuntime {


  private val INSTANTIATION_LOCK = new Object()

  /**
    * Reference to the last created SQLContext.
    */
  @transient private val lastInstantiatedContext = new AtomicReference[SparkRuntime]()

  /**
    * Get the singleton SQLContext if it exists or create a new one using the given SparkContext.
    * This function can be used to create a singleton SQLContext object that can be shared across
    * the JVM.
    */
  def getOrCreate(params: JMap[Any, Any]): SparkRuntime = {
    INSTANTIATION_LOCK.synchronized {
      if (lastInstantiatedContext.get() == null) {
        new SparkRuntime(params)
      }
    }
    PlatformManager.getOrCreate.register(lastInstantiatedContext.get())
    lastInstantiatedContext.get()
  }

  private[platform] def clearLastInstantiatedContext(): Unit = {
    INSTANTIATION_LOCK.synchronized {
      PlatformManager.getOrCreate.unRegister(lastInstantiatedContext.get())
      lastInstantiatedContext.set(null)
    }
  }

  private[platform] def setLastInstantiatedContext(sparkRuntime: SparkRuntime): Unit = {
    INSTANTIATION_LOCK.synchronized {
      lastInstantiatedContext.set(sparkRuntime)
    }
  }
}
