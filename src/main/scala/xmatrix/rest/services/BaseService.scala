package xmatrix.rest.services

import akka.actor.ActorSystem
import xmatrix.core.job.SparkJobOperator
import xmatrix.core.platform.PlatformManager

trait BaseService {
  val runtime = PlatformManager.getRuntime
  val sparkJobOperator = new SparkJobOperator
  implicit val systemActor: ActorSystem = ActorSystem("my-client")

}
