package xmatrix.common

import org.apache.log4j.Logger

import scala.collection.JavaConversions._

object ErrorMsgHandler {
  val logger = Logger.getLogger(getClass.getName)

  def errHandle(msg: String): String = {
    if(!msg.equals("")) {
      logger.error(msg)
    }
    val reg = "\\w*Exception:.*$".r
    val regMatch = reg.findAllIn(msg.split("\n")(0)).toList
    regMatch.length match {
      case 0 =>
        msg
      case 1 =>
        regMatch.get(0)
      case _ =>
        regMatch.last
    }
  }
}
