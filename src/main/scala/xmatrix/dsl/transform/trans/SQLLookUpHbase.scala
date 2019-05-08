package xmatrix.dsl.transform.trans

import xmatrix.dsl.transform.SQLTransform

import java.util

import org.apache.hadoop.hbase.{Cell, HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Row => _, _}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.encoders.RowEncoder

import scala.collection.mutable.ListBuffer
import org.json4s.jackson.Serialization.write
import org.json4s.DefaultFormats


class SQLLookUpHbase extends SQLTransform {

  override def transform(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val viewName = path.split("/").last
    val dfTableList = ListBuffer.empty[Row]
    val BATCHSIZE = params.getOrElse("lookUpBatchSize", "10").toInt
    val newSchema = df.schema.add("lookUp", StringType, false)
    val encoder = RowEncoder(newSchema)

    val resultDF = df.mapPartitions { part => {

      def resolveLookUpKey(lookUpKeyFormat: String, row: Row) = {
        var newKey = ""
        val originKey = lookUpKeyFormat
        try {
          val it = "\\{([^\\}]+)\\}".r.findAllIn(originKey)
          while (it.hasNext) {
            val col = it.next()
            if (!col.isEmpty && !originKey.isEmpty) {
              val rowKey: String = row.getAs(col.replace("{", "").replace("}", ""))
              if (!rowKey.isEmpty && !rowKey.equals("null")) {
                newKey = originKey.replace(col, rowKey)
              }
            }
          }
        } catch {
          case e:Exception =>
            e.printStackTrace()
        }
        newKey
      }

      def resolveResultRow(ret: Result) = {
        var resMap: Map[String, String] = Map.empty[String, String]
        val rowKey = Bytes.toString(ret.getRow)
        ret.listCells().toArray foreach {
          f =>
            val cell = f.asInstanceOf[Cell]
            if (Bytes.toString(cell.getFamily()) == params.getOrElse("columnFamily", "info")) {
              resMap += (Bytes.toString(cell.getQualifier()) -> Bytes.toString(cell.getValue()))
            }
        }
        implicit val formats = DefaultFormats
        (rowKey, write(resMap))
      }

      val tmp = part.toList

      val hbaseConfig = HBaseConfiguration.create()
      hbaseConfig.set("hbase.zookeeper.quorum", params.getOrElse("url", "192.168.13.101,192.168.13.102,192.168.13.103"))
      hbaseConfig.set("hbase.zookeeper.property.clientPort", params.getOrElse("port", "2181"))
      hbaseConfig.set("hbase.client.retries.number", params.getOrElse("retry", "1"))
      hbaseConfig.set("hbase.client.operation.timeout", params.getOrElse("timeout", "10000"))
      hbaseConfig.set("hbase.client.scanner.timeout.period", params.getOrElse("scan.timeout", "60000"))
      val batchQueryKeys = new util.ArrayList[Get]()
      var resultRowMap = Map.empty[String, String]
      var conn: Connection = null
      try {
        conn = ConnectionFactory.createConnection(hbaseConfig)
        tmp.foreach {
          f =>
            val newKey = resolveLookUpKey(params("lookUpKey"), f)
            if (!newKey.equals("")) {
              batchQueryKeys.add(new Get(newKey.getBytes()))
            }

            if (batchQueryKeys.size() >= BATCHSIZE) {
              conn.getTable(TableName.valueOf(params("lookUpTable"))).get(batchQueryKeys).foreach {
                ret: Result =>
                  if (!ret.isEmpty) {
                    resultRowMap += resolveResultRow(ret)
                  }
              }
              batchQueryKeys.clear()
            }
        }
        if (batchQueryKeys.size() > 0) {
          conn.getTable(TableName.valueOf(params("lookUpTable"))).get(batchQueryKeys).foreach {
            ret: Result =>
              if (!ret.isEmpty) {
                resultRowMap += resolveResultRow(ret)
              }
          }
        }
        conn.close()
      } catch {
        case e:Exception =>
          e.printStackTrace()
      } finally {
        if (!conn.equals(null)) {
          conn.close()
        }
      }
      tmp.foreach {
        row =>
          val newKey = resolveLookUpKey(params("lookUpKey"), row)
          val resultCol = resultRowMap.get(newKey) match {
            case Some(i) => i
            case None => "null"
          }
          dfTableList.append(Row.fromSeq(row.toSeq ++ Array(resultCol)))
      }
    }
      dfTableList.toIterator
    }(encoder)

    df.unpersist()
    resultDF
  }
}
