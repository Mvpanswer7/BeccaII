package xmatrix.dsl.transform.trans

import java.sql.{Connection, DriverManager, ResultSet}
import java.util

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.json4s.{DefaultFormats, JObject}
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.Serialization.write
import xmatrix.dsl.ScriptSQLExec
import xmatrix.dsl.transform.SQLTransform

import scala.collection.mutable.ListBuffer

class SQLLookUpMysql extends SQLTransform {

  override def transform(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val viewName = path.split("/").last

    val db = params("lookUpTable").split("\\.")(0)
    val tableName = params("lookUpTable").split("\\.")(1)
    val dbAndTable = ScriptSQLExec.dbMapping.get(db)

    val dfTableList = ListBuffer.empty[Row]
    val BATCHSIZE = params.getOrElse("lookUpBatchSize", "2").toInt
    var newSchema = df.schema
    val newField = ListBuffer.empty[String]
    params("mergeCols").split(",") foreach {
      f =>
        if(!newSchema.fields.distinct.contains(new StructField(f, StringType))) {
          newSchema = newSchema.add(f, StringType)
          newField.append(f)
        }
    }

    val encoder = RowEncoder(newSchema)

    val resultDF = df.mapPartitions { part => {
      def resolveLookUpKey(lookUpKeyFormat: String, row: Row) = {
        var newKey = ""
        val originKey = lookUpKeyFormat
        try {
          "\\{([^\\}]+)\\}".r.findAllIn(originKey).toArray foreach {
            col =>
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

      val tmp = part.toList

      val batchQueryKeys = new util.ArrayList[String]()
      var resultRowMap = Map.empty[String, String]
      var conn: Connection = null
      try {
        classOf[com.mysql.jdbc.Driver]
        conn = DriverManager.getConnection(dbAndTable("url"), dbAndTable("user"), dbAndTable("password"))

        val inClause = new StringBuilder()
        var firstRow = true
        tmp.foreach {
          f =>
            val newKey = resolveLookUpKey(params("lookUpKey"), f)
            batchQueryKeys.add(newKey)
            if ( firstRow )
              firstRow = false
            else
              inClause.append(',')
            inClause.append('?')
            if (batchQueryKeys.size() >= BATCHSIZE) {
              val stmt = conn.prepareStatement( s"select ${params("mergeCols")} from $tableName where ${params("lookUpCol")} in (" + inClause.toString() + ')')
              1 to batchQueryKeys.size() foreach {
                i =>
                  stmt.setString(i, batchQueryKeys.get(i-1))
              }
              val resultSet = stmt.executeQuery()
              while(resultSet.next()) {
                var rowTmp = Map.empty[String, String]
                1 to resultSet.getMetaData.getColumnCount foreach {
                  f =>
                    rowTmp += (resultSet.getMetaData.getColumnName(f) -> resultSet.getString(f))
                }
                implicit val formats = DefaultFormats
                resultRowMap += (resultSet.getString(params("lookUpCol")) -> write(rowTmp))
              }
              batchQueryKeys.clear()
              inClause.clear()
              firstRow = true
              stmt.close()
            }
        }

        if (batchQueryKeys.size() > 0) {
          val stmt = conn.prepareStatement( s"select ${params("mergeCols")} from $tableName where ${params("lookUpCol")} in (" + inClause.toString() + ')')
          1 to batchQueryKeys.size() foreach {
            i =>
              stmt.setString(i, batchQueryKeys.get(i-1))
          }

          val resultSet = stmt.executeQuery()

          while(resultSet.next()) {
            var rowTmp = Map.empty[String, String]
            1 to resultSet.getMetaData.getColumnCount foreach {
              f =>
                rowTmp += (resultSet.getMetaData.getColumnName(f) -> resultSet.getString(f))
            }
            implicit val formats = DefaultFormats
            resultRowMap += (resultSet.getString(params("lookUpCol")) -> write(rowTmp))
          }
          batchQueryKeys.clear()
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
          val resultCol = ListBuffer.empty[String]
          resultRowMap.get(newKey) match {
            case Some(i) =>
              implicit val formats = DefaultFormats
              val jValue = parse(i, false)
              (0 until jValue.children.length) foreach {
                f =>
                  if(!row.schema.fieldNames.distinct.contains(newField(f))) {
                    resultCol.append((jValue.asInstanceOf[JObject] \ newField(f)).extract[String])
                  }
              }
            case None =>
              (0 to newField.toList.size) foreach {
                i =>
                  resultCol.append("null")
              }
          }
          dfTableList.append(Row.fromSeq(row.toSeq ++ resultCol.toList))
      }
    }
      dfTableList.toIterator
    }(encoder)

    resultDF
  }
}
