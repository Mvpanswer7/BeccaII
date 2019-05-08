package xmatrix.rest

import akka.actor.ActorSystem

object TestApp {
  def main(args: Array[String]): Unit = {
    val system = ActorSystem("my-app")
    RestServer("localhost", 8124, system).start()
  }
}
