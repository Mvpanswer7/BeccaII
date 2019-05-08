package org.apache.spark.sql.execution.streaming

import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.{SQLContext}
import org.apache.spark.sql.sources.StreamSinkProvider
import org.apache.spark.sql.streaming.OutputMode

import java.sql.{Date, PreparedStatement}
import java.util
import java.util.Calendar

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import ru.yandex.clickhouse.{ClickHouseConnection, ClickHouseDataSource}
import ru.yandex.clickhouse.settings.ClickHouseProperties


abstract class ClickHouseSinkProvider extends StreamSinkProvider with Serializable with Logging with DataSourceRegister {

  override def shortName(): String = "clickhouse"

  def partitionFunc: Row => Date =
    row => {
      new java.sql.Date(Calendar.getInstance().getTimeInMillis)
    }

  def maxRetry: Int = 50
  def ignoreThrowable: Throwable => Boolean = (ex: Throwable) => ex match {
    case _: org.apache.http.NoHttpResponseException =>
      log.error(ex.getMessage)
      false
    case _: org.apache.http.conn.HttpHostConnectException =>
      log.error(ex.getMessage)
      false
    case rex: java.lang.RuntimeException =>
      rex.getCause match {
        case _: ru.yandex.clickhouse.except.ClickHouseException =>
          log.error(rex.getMessage)
          false // retry
        case _ =>
          log.error(rex.getMessage)
          true
      }
    case _ => true
  }

  override def createSink(
                           sqlContext: SQLContext,
                           parameters: Map[String, String],
                           partitionColumns: Seq[String],
                           outputMode: OutputMode): ClickHouseSink = {

    val originSchema = parameters.get("schema").get
    val newSchema = new util.ArrayList[StructField] {}
    originSchema.split(",") foreach {
      f=>
        val field = f.split(":")
        newSchema.add(new StructField(field(0), ClickHouseUtils.matchSparkDataType(field(1)) , field(2).toBoolean))
    }
    val schema = StructType.apply(newSchema)

    val clusterName = parameters.get("cluster")
    val dbName = parameters("db")
    val _tableName = parameters("path")
    val dt = parameters("dt")
    val partitionColumnName = parameters("partitionCol")
    val indexCol = Seq(parameters("indexCol"))
    val host = parameters("url")
    val port = parameters.getOrElse("port", 8123).asInstanceOf[Int]
    val userName = parameters("user")
    val password = parameters("password")

    val createTableSql = ClickHouseUtils.createTableIfNotExistsSql(
      schema,
      dbName,
      _tableName,
      partitionColumnName,
      indexCol
    )
    log.info("Create new table sql:")
    log.info(createTableSql)

    val connection = ClickHouseUtils.createConnection(host, port, userName, password)
    try{
      println(createTableSql)
      connection.createStatement().execute(createTableSql)
    }finally {
      connection.close()
      log.info(s"ClickHouse table ${dbName}.${_tableName} created")
    }

    log.info("Creating ClickHouse sink")
    new ClickHouseSink(dbName, _tableName, partitionColumnName)(host,port)(partitionFunc)(maxRetry, ignoreThrowable)(userName)(password)(schema)
  }


}

class ClickHouseSink(dbName: String, tableName: String, eventDataColumn: String)
                                            (hostPort: (String, Int))
                                            (partitionFunc: (org.apache.spark.sql.Row) => java.sql.Date)
                                            (maxRetry: Int, ignoreThrowable: (Throwable) => Boolean)
                                            (userName: String)(password: String)(schema: StructType)
                                             extends Sink with Serializable with Logging {

  override def addBatch(batchId: Long, data: DataFrame) = {

    val res = data.queryExecution.toRdd.mapPartitions{ iter =>
      if(iter.nonEmpty){
        Retry.retrySync(maxRetry, ignoreThrowable = ignoreThrowable){
          val clickHouseHostPort = hostPort
          Utils.using(ClickHouseUtils.createConnection(hostPort._1, hostPort._2, userName, password)){ connection =>
            val insertStatement = ClickHouseUtils.prepareInsertStatement(connection, dbName, tableName, eventDataColumn)(schema)
            iter.foreach{ internalRow =>
              val row = Row.fromSeq(internalRow.toSeq(schema))
              ClickHouseUtils.batchAdd(schema, row)(insertStatement)(partitionFunc)
            }
            val inserted = insertStatement.executeBatch().sum
            log.info(s"Inserted $inserted -> (${clickHouseHostPort._1}:${clickHouseHostPort._2})")

            List(inserted).toIterator
          }
        }
      } else {
        Iterator.empty
      }
    }

    val insertedCount = res.collect().sum
    log.info(s"Batch $batchId's inserted total: $insertedCount")
  }
}

object ClickHouseUtils extends Serializable with Logging {

  def createConnection(host: String, port: Int, userName: String, password: String) = {
    val properties = new ClickHouseProperties()
    properties.setUser(userName)
    properties.setPassword(password)
    val dataSource = new ClickHouseDataSource(s"jdbc:clickhouse://${host}:${port.toInt}", properties)
    dataSource.getConnection()
  }

  def matchSparkDataType(typeStr: String): DataType = {
    typeStr match {
      case "StringType"  => org.apache.spark.sql.types.DataTypes.StringType
      case "IntegerType" => org.apache.spark.sql.types.DataTypes.IntegerType
      case "FloatType"   => org.apache.spark.sql.types.DataTypes.FloatType
      case "LongType"    => org.apache.spark.sql.types.DataTypes.LongType
      case "BooleanType" => org.apache.spark.sql.types.DataTypes.BooleanType
      case "DoubleType"  => org.apache.spark.sql.types.DataTypes.DoubleType
      case "DateType"    => org.apache.spark.sql.types.DataTypes.DateType
      case "TimestampType" => org.apache.spark.sql.types.DataTypes.TimestampType
      case _ => throw new IllegalArgumentException(s"Unsupported type $typeStr")
    }
  }

  def createTableIfNotExistsSql(schema: StructType, dbName: String, tableName: String, eventDateColumnName: String, indexColumns: Seq[String]) = {

    val tableFieldsStr = schema.map{ f =>

      val fName = f.name
      val fType = f.dataType match {
        case org.apache.spark.sql.types.DataTypes.StringType => "String"
        case org.apache.spark.sql.types.DataTypes.IntegerType => "Int32"
        case org.apache.spark.sql.types.DataTypes.FloatType => "Float32"
        case org.apache.spark.sql.types.DataTypes.LongType => "Int64"
        case org.apache.spark.sql.types.DataTypes.BooleanType => "UInt8"
        case org.apache.spark.sql.types.DataTypes.DoubleType => "Float64"
        case org.apache.spark.sql.types.DataTypes.DateType => "DateTime"
        case org.apache.spark.sql.types.DataTypes.TimestampType => "DateTime"
        case x => throw new Exception(s"Unsupported type: ${x.toString}")
      }

      s"$fName $fType"

    }.mkString(", ")

    val createTableSql = s"""CREATE TABLE IF NOT EXISTS ${dbName}.${tableName} ($eventDateColumnName Date, $tableFieldsStr) ENGINE = MergeTree($eventDateColumnName, (${indexColumns.mkString(",")}), 8192)"""

    // return
    createTableSql
  }

  def prepareInsertStatement(connection: ClickHouseConnection, dbName: String, tableName: String, partitionColumnName: String)
                            (schema: org.apache.spark.sql.types.StructType) = {

    val insertSql = createInsertStatmentSql(dbName, tableName, partitionColumnName)(schema)
    log.debug("ClickHouse insert sql:")
    log.debug(insertSql)
    connection.prepareStatement(insertSql)
  }

  def batchAdd(schema: org.apache.spark.sql.types.StructType, row: Row)
              (statement: PreparedStatement)
              (partitionFunc: (org.apache.spark.sql.Row) => java.sql.Date)= {

    val partitionVal = partitionFunc(row)
    statement.setDate(1, partitionVal)

    schema.foreach{ f =>
      val fieldName = f.name
      val fieldIdx = schema.fieldIndex(fieldName)
      val fieldVal = row.get(fieldIdx)
      if(fieldVal != null) {

        //对原生 UTF8String 类型的支持
        if(fieldVal.isInstanceOf[org.apache.spark.unsafe.types.UTF8String]){
          statement.setObject(fieldIdx + 2, fieldVal.toString)
        } else {
          statement.setObject(fieldIdx + 2, fieldVal)
        }
      }
      else{
        val defVal = defaultNullValue(f.dataType, fieldVal)
        statement.setObject(fieldIdx + 2, defVal)
      }
    }

    statement.addBatch()
  }

  private def createInsertStatmentSql(dbName: String, tableName: String, partitionColumnName: String)
                                     (schema: org.apache.spark.sql.types.StructType) = {

    val columns = partitionColumnName :: schema.map(f => f.name).toList
    val vals = 1 to (columns.length) map (i => "?")
    s"INSERT INTO $dbName.$tableName (${columns.mkString(",")}) VALUES (${vals.mkString(",")})"
  }

  private def defaultNullValue(sparkType: org.apache.spark.sql.types.DataType, v: Any) = sparkType match {

    case DoubleType => 0
    case LongType => 0
    case FloatType => 0
    case IntegerType => 0
    case StringType => null
    case BooleanType => false
    case _ => null
  }

}

class CKSinkProvider extends ClickHouseSinkProvider {

}