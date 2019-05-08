package org.apache.spark.sql.execution.streaming

import java.sql.{Connection, DriverManager, PreparedStatement}
import org.apache.spark.sql.ForeachWriter

class JDBCWriter(url: String, user: String, passward: String, table: String, opt: String) extends ForeachWriter[org.apache.spark.sql.Row] {
  val driver = "com.mysql.jdbc.Driver"
  var connection: Connection = _
  var statement: PreparedStatement = _

  def open(partitionId: Long, version: Long): Boolean = {
    Class.forName(driver)
    connection = DriverManager.getConnection(url, user, passward)
    true
  }

  def process(value: org.apache.spark.sql.Row): Unit = {
    val columnStr = value.schema.fields.map(f => s"`${f.name}`").mkString(",")
    val valueStr = List.fill(value.schema.fields.length)("?").mkString(", ")
    //val updateStr = columns.map(c => s"`${c}`=values(${c})").mkString(", ")
    var preSql=  s"""insert into $table ($columnStr) values($valueStr)"""
    opt match {
      case "replace" => preSql=  s"""replace into $table ($columnStr) values($valueStr)"""
      case _ =>
    }
    statement = connection.prepareStatement(preSql)
    val rows = value.toSeq.map(v => {
      if (v == null) {""}
      else {v.toString}
    })
    for(i <- 1 to rows.length) {
      statement.setString(i, rows(i-1))
    }
    try {
      statement.executeUpdate()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      statement.close()
    }
  }

  def close(errorOrNull: Throwable): Unit = {
    connection.close()
  }
}
