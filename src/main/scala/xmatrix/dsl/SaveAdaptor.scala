package xmatrix.dsl

import java.sql.Date
import java.util.concurrent.TimeUnit

import org.apache.log4j.Logger
import org.apache.spark.sql._
import xmatrix.dsl.parser.DSLSQLParser._
import xmatrix.dsl.template.TemplateMerge
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.streaming.{DataStreamWriter, Trigger}
import org.apache.spark.sql.execution.streaming.JDBCWriter
import xmatrix.core.job._

class SaveAdaptor(scriptSQLExecListener: ScriptSQLExecListener) extends DslAdaptor {

  def evaluate(value: String) = {
    TemplateMerge.merge(value, scriptSQLExecListener.env().toMap)
  }

  override def parse(ctx: SqlContext): Unit = {

    var oldDF: DataFrame = null
    var mode = SaveMode.ErrorIfExists
    var final_path = ""
    var format = ""
    var option = Map[String, String]()
    var tableName = ""
    var partitionByCol = Array[String]()

    val owner = option.get("owner")

    (0 until ctx.getChildCount).foreach { tokenIndex =>
      ctx.getChild(tokenIndex) match {
        case s: FormatContext =>
          format = s.getText
          format match {
            case "hive" =>
            case _ =>
              format = s.getText
          }


        case s: PathContext =>
          format match {
            case "hive" | "kafka8" | "kafka9" | "hbase" | "redis" | "es" | "jdbc" =>
              final_path = cleanStr(s.getText)
            case "parquet" | "json" | "csv" | "orc" =>
              final_path = withPathPrefix(scriptSQLExecListener.pathPrefix(owner), cleanStr(s.getText))
            case _ =>
              final_path = cleanStr(s.getText)
          }

          final_path = TemplateMerge.merge(final_path, scriptSQLExecListener.env().toMap)
          val operator = new SparkJobOperator
          val jobEventInfo = new JobEventInfo(scriptSQLExecListener.groupId, -1, -1, -1, "", "", -1,-1, SparkJobEventType.onUpdatePath, "")
          operator.updateJobRuntimeState(jobEventInfo, Map("jobType" -> scriptSQLExecListener.env().getOrElse("jobType", SparkJobType.SCRIPT), "path" -> final_path))

        case s: TableNameContext =>
          //TODO
          tableName = scriptSQLExecListener.resolveTableName(s.getText)
          oldDF = scriptSQLExecListener.sparkSession.table(tableName)
        case s: OverwriteContext =>
          mode = SaveMode.Overwrite
        case s: AppendContext =>
          mode = SaveMode.Append
        case s: ErrorIfExistsContext =>
          mode = SaveMode.ErrorIfExists
        case s: IgnoreContext =>
          mode = SaveMode.Ignore
        case s: ColContext =>
          partitionByCol = cleanStr(s.getText).split(",")
        case s: ExpressionContext =>
          option += (cleanStr(s.identifier().getText) -> evaluate(cleanStr(s.STRING().getText)))
        case s: BooleanExpressionContext =>
          option += (cleanStr(s.expression().identifier().getText) -> evaluate(cleanStr(s.expression().STRING().getText)))
        case _ =>
      }
    }

    if (scriptSQLExecListener.env().contains("stream")) {
      if (option.contains("checkpointLocation")) {
        option += ("checkpointLocation" -> withPathPrefix(scriptSQLExecListener.pathPrefix(owner), option("checkpointLocation")))
      } else if (scriptSQLExecListener.checkpoint){
        option += ("checkpointLocation" -> withPathPrefix(scriptSQLExecListener.pathPrefix(owner),  s"checkpoint/${SparkJobManager.getGroupJobInfo(scriptSQLExecListener.groupId).get.jobName}/"))
      }
      new StreamSaveAdaptor(scriptSQLExecListener, option, oldDF, final_path, tableName, format, mode, partitionByCol).parse
    } else {
      new BatchSaveAdaptor(scriptSQLExecListener, option, oldDF, final_path, tableName, format, mode, partitionByCol).parse
    }
    //scriptSQLExecListener.setLastSelectTable(null)
  }
}

class BatchSaveAdaptor(val scriptSQLExecListener: ScriptSQLExecListener,
                       var option: Map[String, String],
                       var oldDF: DataFrame,
                       var final_path: String,
                       var tableName: String,
                       var format: String,
                       var mode: SaveMode,
                       var partitionByCol: Array[String]
                      ) {
  def parse = {

    if (option.contains("fileNum")) {
      oldDF = oldDF.repartition(option.getOrElse("fileNum", "").toString.toInt)
    }

    var writer = oldDF.write
    val dbAndTable = final_path.split("\\.")
    var connect_provied = false
    if (dbAndTable.length == 2 && ScriptSQLExec.dbMapping.containsKey(dbAndTable(0))) {
      ScriptSQLExec.dbMapping.get(dbAndTable(0)).foreach {
        f =>
          writer.option(f._1, f._2)
      }
      connect_provied = true
    }

    if (connect_provied) {
      final_path = dbAndTable(1)
    }


    writer = writer.format(format).mode(mode).partitionBy(partitionByCol: _*).options(option)
    format match {
      case "es" =>
        writer.save(final_path)
      case "hive" =>
        writer.format(option.getOrElse("file_format", "parquet"))
        writer.saveAsTable(final_path)
      case "kafka8" | "kafka9" =>
        writer.option("topics", final_path).format("com.hortonworks.spark.sql.kafka08").save()
      case "kafka" =>
        if (dbAndTable.size == 2) {
          writer.format(option.getOrElse("implClass", format)).option("topic", dbAndTable(1)).save(final_path)
        } else {
          writer.format(option.getOrElse("implClass", format)).save(final_path)
        }
      case "hbase" =>
        writer.option("outputTableName", final_path).format(
          option.getOrElse("implClass", "org.apache.spark.sql.execution.datasources.hbase")).save()
      case "redis" =>
        writer.option("outputTableName", final_path).format(
          option.getOrElse("implClass", "org.apache.spark.sql.execution.datasources.redis")).save()
      case "jdbc" =>
        import org.apache.spark.sql.jdbc.DataFrameWriterExtensions._
        val extraOptionsField = writer.getClass.getDeclaredField("extraOptions")
        extraOptionsField.setAccessible(true)
        val extraOptions = extraOptionsField.get(writer).asInstanceOf[scala.collection.mutable.HashMap[String, String]]
        val jdbcOptions = new JDBCOptions(extraOptions.toMap + ("dbtable" -> final_path))
        writer.upsert(option.get("idCol"), jdbcOptions, oldDF)
      case "carbondata" =>
        if (dbAndTable.size == 2) {
          writer.option("tableName", dbAndTable(1)).option("dbName", dbAndTable(0))
        }
        if (dbAndTable.size == 1 && dbAndTable(0) != "-") {
          writer.option("tableName", dbAndTable(0))
        }
        writer.format(option.getOrElse("implClass", "carbondata")).save()
      case "kudu" =>
        //2018.08.30, save grammar add kudu data source
        import org.apache.spark.sql.types.{StructField, StructType}
        import org.apache.kudu.spark.kudu.KuduContext
        import org.apache.kudu.client.CreateTableOptions
        import collection.JavaConverters._
        val kuduConfig = ScriptSQLExec.dbMapping.get(dbAndTable(0))
        val kc = new KuduContext(kuduConfig("kudu.master"), scriptSQLExecListener.sparkSession.sparkContext)
        if (!kc.tableExists(final_path)) {
          // 2018.08.31, fix json file to data frame, nullable default = true, create kudu table failed
          val newSchema = StructType(oldDF.schema.map {
            case StructField(c, t, _, m) if c.equals(option("kudu.primaryKey")) => StructField(c, t, nullable=false, m)
            case y: StructField => y
          })
          kc.createTable(
            final_path, newSchema, Seq(option("kudu.primaryKey")),
            new CreateTableOptions()
              .setNumReplicas(option("kudu.replicas").toInt)
              .addHashPartitions(
                List(option("kudu.hashPartitionsColumns")).asJava,
                option("kudu.hashPartitionsBuckets").toInt))
        }
        writer.option("kudu.table", final_path)
        writer.format("org.apache.kudu.spark.kudu").save()
      case "ck" =>
        import org.apache.spark.sql.execution.datasources.clickhouse._
        import org.apache.spark.sql.execution.datasources.clickhouse.spark._

        val connInfo = ScriptSQLExec.dbMapping.get(dbAndTable(0))
        val host = connInfo("url")
        val port = connInfo.getOrElse("port", "8123").toInt
        val userName = connInfo("user")
        val password = connInfo("password")
        ClickhouseConnectionFactory.setUserName(userName)
        ClickhouseConnectionFactory.setPassword(password)
        implicit val clickhouseDataSource = ClickhouseConnectionFactory.get(host, port, userName, password)

        val df = ClickhouseSparkExt.extraOperations(oldDF)
        val clusterName = option.get("cluster")
        val dbName = option("db")
        val tableName = dbAndTable.last
        val dt = option.getOrElse("dt", new Date(System.currentTimeMillis()).toString)
        val partitionColumnName = option("partitionCol")
        val indexCol = option("indexCol")

        df.createClickhouseTable(dbName, tableName, userName, password, partitionColumnName, Seq(indexCol), clusterName)(clickhouseDataSource)
        df.saveToClickhouse(dbName, tableName, userName, password, (row) => java.sql.Date.valueOf(dt), partitionColumnName, clusterName)
      case _ =>
        if (final_path == "-" || final_path.isEmpty) {
          writer.format(option.getOrElse("implClass", format)).save()
        } else {
          writer.format(option.getOrElse("implClass", format)).save(final_path)
        }

    }
  }
}

class StreamSaveAdaptor(val scriptSQLExecListener: ScriptSQLExecListener,
                        var option: Map[String, String],
                        var oldDF: DataFrame,
                        var final_path: String,
                        var tableName: String,
                        var format: String,
                        var mode: SaveMode,
                        var partitionByCol: Array[String]
                       ) {
  val logger = Logger.getLogger(getClass.getName)
  def parse = {
    if (option.contains("fileNum")) {
      oldDF = oldDF.repartition(option.getOrElse("fileNum", "").toString.toInt)
    }
    var writer: DataStreamWriter[Row] = oldDF.writeStream
    val dbAndTable = final_path.split("\\.")
    if (ScriptSQLExec.dbMapping.containsKey(dbAndTable(0))) {
      ScriptSQLExec.dbMapping.get(dbAndTable(0)).foreach { f =>
        writer.option(f._1, f._2)
      }
      final_path = dbAndTable(1)
    }

    require(option.contains("duration"), "duration is required")

    val dbtable = if (option.contains("dbtable")) option("dbtable") else final_path

    if (option.getOrElse("implClass", "") == "foreachSink") {
      require(option.contains("writerClazz"), "foreachWrite is required")
      //"org.apache.spark.sql.execution.streaming.JDBCWriter"

      val clazz = option("writerClazz") match {
        case "org.apache.spark.sql.execution.streaming.JDBCWriter" =>
          val extraOptionsField = writer.getClass.getDeclaredField("extraOptions")
          extraOptionsField.setAccessible(true)
          val extraOptions = extraOptionsField.get(writer).asInstanceOf[scala.collection.mutable.HashMap[String, String]]
          val jdbcOptions = new JDBCOptions(extraOptions.toMap + ("dbtable" -> final_path))
          new JDBCWriter(jdbcOptions.url, extraOptions.getOrElse("user", ""),extraOptions.getOrElse("password", ""), jdbcOptions.table, option.getOrElse("opt", "insert"))
      }
      writer = writer.foreach(clazz)
        .outputMode(option.getOrElse("mode", "upsert"))
        .options((option - "mode" - "duration"))
    } else {
      writer = writer.format(option.getOrElse("implClass", format))
        .outputMode(option.getOrElse("mode", "append"))
        .partitionBy(partitionByCol: _*).
        options((option.+("format" -> format)))
    }

    if (dbtable != null && dbtable != "-") {
      format match {
        case "kafka" =>
          writer.option("topic", dbtable)
        case "ck" =>
          writer.option("path", dbtable)
          writer.option("schema", schemaToString(oldDF.schema))
          writer.option("implClass", "org.apache.spark.sql.execution.streaming.CKSinkProvider")
        case "kudu" =>
          writer.option("path", dbtable)
          writer.option("implClass", "org.apache.spark.sql.execution.streaming.KuduSinkProvider")
        case "hbase" =>
          writer.option("implClass", "org.apache.spark.sql.execution.streaming.HBaseSinkProvider")
          writer.option("path", dbtable)
        case _ =>
          writer.option("path", dbtable)
      }
    }


    scriptSQLExecListener.env().get("streamName") match {
      case Some(name) => writer.queryName(name)
      case None =>
    }
    if (option.getOrElse("continuous", "false").toBoolean) {
      writer.trigger(Trigger.Continuous(option("duration").toInt, TimeUnit.SECONDS)).start().awaitTermination()
    } else {
      writer.trigger(Trigger.ProcessingTime(option("duration").toInt, TimeUnit.SECONDS)).start().awaitTermination()
    }
  }

  import org.apache.spark.sql.types.StructType
  def schemaToString(schema: StructType): String = {
    val fieldMap = schema.fields.map(f=>s"${f.name}:${f.dataType}:${f.nullable}")
    fieldMap.mkString(",")
  }

}
