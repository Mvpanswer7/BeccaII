package xmatrix.rest.entities

/**
  * Created by iodone on {18-5-7}.
  */

import scala.collection.mutable.Map

sealed trait Entity

final case class Response(meta: Map[String, Any], data: Any)

// order entity
final case class Item(name: String, id: Long)
final case class Order(id: Long, items: List[Item])

// query entity
final case class XqlQuery(async: String,
                          owner: String,
                          var jobName: String,
                          sql: String,
                          timeout: Long = -1,
                          var groupId: String = "",
                          pool: String = "default",
                          allPathPrefix: String = "{}",
                          defaultPathPrefix: String = "",
                          callback: String = "" ,
                          checkpoint: Option[String] = Some("false"),
                          startingOffsets: String
                         )

final case class SqlQuery(async: String, owner: String, sql: String, jobName: String, timeout: Long = -1)

// job entity
final case class Job(jobType: String, groupId: String = "")
final case class JobList(groupIds: List[String])
final case class UdfRegister(path:String, className: String)
final case class XqlSyntaxCheck(sql: String)
final case class Test(input: String)



