package xmatrix.rest

/**
  * Created by iodone on {18-5-10}.
  */

import akka.actor.ActorSystem

object RestApp {
  def main(args: Map[String, String]): Unit = {
    val system = ActorSystem("my-app")
    val ip = args.getOrElse("ip", "0.0.0.0")
    val port = args.getOrElse("port", "").toInt
    RestServer(ip, port, system).start()
  }
}
