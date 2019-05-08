package xmatrix.rest

/**
  * Created by iodone on 2018/5/4.
  */

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.stream.ActorMaterializer
import xmatrix.rest.routing.Router
import scala.concurrent.{ExecutionContext, Future}


class RestServer(host: String,
                 port: Int,
                 actorSystem: ActorSystem) extends Router {

  implicit val system: ActorSystem = actorSystem
  implicit val executionContext: ExecutionContext = system.dispatcher
  implicit val materializer = ActorMaterializer()

  var bindingFuture: Future[ServerBinding] = _

  def start(): Unit = {
    bindingFuture = Http().bindAndHandle(routes, host, port)
    println(s"Waiting for requests at http://$host:$port/...")
  }

  def stop(): Unit = {
    if (bindingFuture != null) {
      bindingFuture
        .flatMap(_.unbind())
        .onComplete(_ => system.terminate)
    }
  }

}

object RestServer {
  def apply(host: String, port: Int, actorSystem: ActorSystem): RestServer =
    new RestServer(host, port, actorSystem)
}
