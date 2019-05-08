package xmatrix.rest.routing

/**
  * Created by iodone on 2018/5/4.
  */


import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.model._
import xmatrix.rest.serializers.JsonSupport
import xmatrix.rest.entities._
import xmatrix.rest.services.{OrderService, QueryService, HelperService, UdfService}
import org.json4s._
import scala.concurrent.duration._


trait Router extends JsonSupport with OrderService with QueryService with UdfService with HelperService{

  val routes: Route =
  //    pathPrefix("sql") {
  //      pathPrefix("batch") {
  //        path("query")(post(entity(as[SqlQuery])(query => complete(sqlQuery(query))))) ~
  //          path("progress")(post(entity(as[SqlProgress])(progress => complete(sqlProgress(progress))))) ~
  //          path("result")(post(entity(as[SqlResult])(result => complete(sqlResult(result)))))
  //      } ~
  //        pathPrefix("adhoc") {
  //          path("open")(post(complete(sqlOpenSession(SqlOpenSession)))) ~
  //            path("query")(post(entity(as[SqlQuery])(query => complete(sqlQuery(query))))) ~
  //            path("progress")(post (entity(as[SqlProgress])(progress => complete(sqlProgress(progress))))) ~
  //            path("result")(post (entity(as[SqlResult])(result => complete(sqlResult(result))))) ~
  //            path("cancel")(post (entity(as[SqlCancel])(result => complete(sqlCancel(result))))) ~
  //            path("close")(post (entity(as[SqlCloseSession])(close => complete(sqlCloseSession(close)) )))
  //        }
  //    }

    pathPrefix("api") {
      //pathPrefix("test") {
      //  complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1>Say hello to akka-http</h1>"))
      //} ~
      pathPrefix("order") {
        path("get-items")(post(entity(as[JValue]) { jvalue =>

          val ids = for {
            JObject(item) <- jvalue
            JField("id", JInt(id)) <- item
          } yield id

          val respF = fetchItem(ids.head.toLong)
          onComplete(respF) { respTry =>
            val resp = respTry.get
            complete(resp.meta.getOrElse("statusCode", 200).asInstanceOf[Int] -> resp)
          }
        })) ~
          path("save")(post(entity(as[Order]) { order =>
            val respF = saveOrder(order)
            onComplete(respF) { resp =>
              //              complete(HttpEntity(ContentTypes.`application/json`, serialization.write(resp)))
              complete(resp)
            }
          }))
      } ~
        pathPrefix("query") {
          path("sql")(post(entity(as[SqlQuery]) { req =>
            val resp = querySql(req)
            complete(resp.meta.getOrElse("statusCode", 200).asInstanceOf[Int] -> resp)
          })) ~
            path("xql-test") {
              toStrictEntity(5.seconds) {
                formFields('async.as[String], 'owner.as[String], 'jobName.as[String], 'sql.as[String], 'timeout.as[Int], 'groupId.as[String], 'pool.as[String], 'defaultPathPrefix.as[String], 'callback.as[String], 'startingOffsets.as[String]) {
                  (async, owner, jobName, sql, timeout, groupId, pool, defaultPathPrefix, callback, startingOffsets) =>
                    val resp = testXql(async, owner, jobName, sql, timeout, groupId, pool, defaultPathPrefix, callback, startingOffsets)
                    complete(resp.meta.getOrElse("statusCode", 200).asInstanceOf[Int] -> resp)
                }
              }
            } ~
            path("xql"){
              toStrictEntity(5.seconds) {
                (post( entity(as[XqlQuery]) { req =>
                  val resp = queryXqlV2(req)
                  complete(resp.meta.getOrElse("statusCode", 200).asInstanceOf[Int] -> resp)
                }))
              }
            } ~
          path("check"){
            toStrictEntity(5.seconds) {
              (post( entity(as[XqlSyntaxCheck]) { req =>
                val resp = checkSyntax(req.sql)
                complete(resp.meta.getOrElse("statusCode", 200).asInstanceOf[Int] -> resp)
              }))
            }
          }
        } ~
        pathPrefix("udf") {
          path("register")(post(entity(as[UdfRegister]) { req =>
            val resp = registerUdf(req)
            complete(resp.meta.getOrElse("statusCode", 200).asInstanceOf[Int] -> resp)
          }))
        } ~
        pathPrefix("job") {
          path("listall")(post(entity(as[JobList]) { req =>
            val resp = getRunningJobList(req)
            complete(resp.meta.getOrElse("statusCode", 200).asInstanceOf[Int] -> resp)
          })) ~
            path("list")(post(entity(as[Job]) { req =>
              val resp = getRunningJob(req)
              complete(resp.meta.getOrElse("statusCode", 200).asInstanceOf[Int] -> resp)
            })) ~
            path("kill")(post(entity(as[Job]) { req =>
              val resp = killRunningJob(req)
              complete(resp.meta.getOrElse("statusCode", 200).asInstanceOf[Int] -> resp)
            })) ~
            path("remove")(post(entity(as[Job]) { req =>
              val resp = removeJobInfo(req)
              complete(resp.meta.getOrElse("statusCode", 200).asInstanceOf[Int] -> resp)
            }))
        }
    }
}