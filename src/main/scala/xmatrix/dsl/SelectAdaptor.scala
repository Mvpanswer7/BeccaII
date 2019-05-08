package xmatrix.dsl

import java.util

import org.antlr.v4.runtime.misc.Interval
import org.apache.hadoop.hbase.{Cell, HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Row => _, _}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.Logger
import xmatrix.dsl.parser.DSLSQLLexer
import xmatrix.dsl.parser.DSLSQLParser.{BooleanExpressionContext, ExpressionContext, SqlContext}
import xmatrix.dsl.template.TemplateMerge
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.encoders.RowEncoder

import scala.collection.mutable.ListBuffer
import org.json4s.jackson.Serialization.write
import org.json4s.DefaultFormats


class SelectAdaptor(scriptSQLExecListener: ScriptSQLExecListener) extends DslAdaptor {
  val logger = Logger.getLogger(getClass.getName)

  override def parse(ctx: SqlContext): Unit = {
    val input = ctx.start.getTokenSource().asInstanceOf[DSLSQLLexer]._input
    var option = Map[String, String]()

    (0 to ctx.getChildCount() - 1).foreach { tokenIndex =>
      ctx.getChild(tokenIndex) match {
        case s: ExpressionContext =>
          option += (cleanStr(s.identifier().getText) -> evaluate(cleanStr(s.STRING().getText)))
        case s: BooleanExpressionContext
        =>
          option += (cleanStr(s.expression().identifier().getText) -> evaluate(cleanStr(s.expression().STRING().getText)))
        case _ =>
      }
    }

    val start = ctx.start.getStartIndex()
    val stop = ctx.stop.getStopIndex()
    val interval = new Interval(start, stop)
    val originalText = input.getText(interval).replaceAll("options.*?$", "")
    val chunks = originalText.split("\\s+")
    val originTableName = chunks.last.replace(";", "")

    val tableName = scriptSQLExecListener.resolveTableName(originTableName)
    var sql = originalText.replaceAll(s"as[\\s|\\n]+${originTableName}", "")

    sql = TemplateMerge.merge(sql, scriptSQLExecListener.env().toMap)
    sql = scriptSQLExecListener.replaceTableName(sql)
    logger.info(s"Final sql: $sql")
    var df = waterMark(scriptSQLExecListener.sparkSession.sql(sql), option)

    df = withLookUp(df, option)
    df.createOrReplaceTempView(tableName)
    scriptSQLExecListener.setLastSelectTable(tableName)
  }

  def evaluate(value: String) = {
    TemplateMerge.merge(value, scriptSQLExecListener.env().toMap)
  }

  def waterMark(table: DataFrame, option: Map[String, String]) = {
    if (option.contains("eventTimeCol")) {
      table.withWatermark(option("eventTimeCol"), option("delayThreshold"))
    } else {
      table
    }
  }

  def withLookUp(table: DataFrame, option: Map[String, String]): DataFrame = {
    if(option.contains("lookUpKey")) {
      val dfTableList = ListBuffer.empty[Row]
      val BATCHSIZE = option.getOrElse("lookUpBatchSize", "10").toInt
      val newSchema = table.schema.add("lookUp", StringType, false)
      val encoder = RowEncoder(newSchema)

      val resultDF = table.mapPartitions { part => {

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
              if (Bytes.toString(cell.getFamily()) == option.getOrElse("columnFamily", "info")) {
                resMap += (Bytes.toString(cell.getQualifier()) -> Bytes.toString(cell.getValue()))
              }
          }
          implicit val formats = DefaultFormats
          (rowKey, write(resMap))
        }

        val tmp = part.toList

        val hbaseConfig = HBaseConfiguration.create()
        hbaseConfig.set("hbase.zookeeper.quorum", option.getOrElse("url", "192.168.13.101,192.168.13.102,192.168.13.103"))
        hbaseConfig.set("hbase.zookeeper.property.clientPort", option.getOrElse("port", "2181"))
        hbaseConfig.set("hbase.client.retries.number", option.getOrElse("retry", "1"))
        hbaseConfig.set("hbase.client.operation.timeout", option.getOrElse("timeout", "10000"))
        hbaseConfig.set("hbase.client.scanner.timeout.period", option.getOrElse("scan.timeout", "60000"))
        val batchQueryKeys = new util.ArrayList[Get]()
        var resultRowMap = Map.empty[String, String]
        var conn: Connection = null
        try {
          conn = ConnectionFactory.createConnection(hbaseConfig)
          tmp.foreach {
            f =>
              val newKey = resolveLookUpKey(option("lookUpKey"), f)
              if (!newKey.equals("")) {
                batchQueryKeys.add(new Get(newKey.getBytes()))
              }

              if (batchQueryKeys.size() >= BATCHSIZE) {
                conn.getTable(TableName.valueOf(option("lookUpTable"))).get(batchQueryKeys).foreach {
                  ret: Result =>
                    if (!ret.isEmpty) {
                      resultRowMap += resolveResultRow(ret)
                    }
                }
                batchQueryKeys.clear()
              }
          }
          if (batchQueryKeys.size() > 0) {
            conn.getTable(TableName.valueOf(option("lookUpTable"))).get(batchQueryKeys).foreach {
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
            val newKey = resolveLookUpKey(option("lookUpKey"), row)
            val resultCol = resultRowMap.get(newKey) match {
              case Some(i) => i
              case None => "null"
            }
            dfTableList.append(Row.fromSeq(row.toSeq ++ Array(resultCol)))
        }
      }
        dfTableList.toIterator
      }(encoder)

      table.unpersist()
      resultDF
    } else {

      table
    }
  }


}
