package xmatrix.rest.services

/**
  * Created by iodone on {18-5-7}.
  */


import scala.concurrent.Future
import scala.collection.mutable.Map
import scala.concurrent.ExecutionContext.Implicits.global
//import xmatrix.rest.serializers.JsonSupport

import org.json4s._
import org.json4s.jackson.JsonMethods._

import xmatrix.rest.entities._

trait OrderService {

  var orders: List[Item] = Nil

  def saveOrder(order: Order): Future[Response] = {
    orders = order match {
      case Order(_, items) => items ::: orders
      case _ => orders
    }

    val meta = Map("reqId" -> "req_id_0",
        "statusCode" -> 200,
        "errorCode" -> 0,
        "errorMsg" -> "")

    val data = Map("id" -> order.id,
        "ok" -> "ok")

    // mock async process
    Future { Response(meta, data) }

  }

  def fetchItem(id: Long): Future[Response] = Future {
    val items = orders.find(_.id == id)
    val meta = Map("reqId" -> "req_id_1",
      "statusCode" -> 404,
      "errorCode" -> 0,
      "errorMsg" -> "")

    val data = items

    Response(meta, data)
  }
}
