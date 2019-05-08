package xmatrix.core.platform

/**
  * Created by iodone on {18-5-10}.
  */

import java.util.{Map => JMap}


trait XmatrixRuntime {

  def startRuntime: XmatrixRuntime

  def destroyRuntime(stopGraceful: Boolean, stopContext: Boolean = false): Boolean

  def xmatrixRuntimeInfo: XmatrixRuntimeInfo

  def resetRuntimeOperator(runtimeOperator: RuntimeOperator)

  def configureXmatrixRuntimeInfo(xmatrixRuntimeInfo: XmatrixRuntimeInfo)

  def awaitTermination

  def startThriftServer

  def startHttpServer

  def params: JMap[Any, Any]

}

trait XmatrixRuntimeInfo

trait Event

trait RuntimeOperator

// case class JobFlowGenerate(jobName: String, index: Int, strategy: Strategy[Any]) extends Event

trait PlatformManagerListener {
  def processEvent(event: Event)
}

