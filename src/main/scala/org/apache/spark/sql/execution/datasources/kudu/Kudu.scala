package org.apache.spark.sql.execution.datasources.kudu

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.kudu.client.CreateTableOptions

import collection.JavaConverters._

//object Kudu {
//  def createTable(kuduConfig: ConcurrentHashMap[String, String]) = {
//    val kc = new KuduContext(kuduConfig("kudu.master"), scriptSQLExecListener.sparkSession.sparkContext)
//
//  }
//}
