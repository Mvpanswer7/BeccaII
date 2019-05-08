package xmatrix.dsl

import org.apache.log4j.Logger
import org.apache.spark.sql._
import xmatrix.dsl.parser.DSLSQLParser._
import template.TemplateMerge
import xmatrix.common.HDFSOperator
import xmatrix.core.job.SparkJobManager
import xmatrix.core.platform.PlatformManager
import xmatrix.core.platform.SparkRuntime

class LoadAdaptor(scriptSQLExecListener: ScriptSQLExecListener) extends DslAdaptor {

  def evaluate(value: String) = {
    TemplateMerge.merge(value, scriptSQLExecListener.env().toMap)
  }

  override def parse(ctx: SqlContext): Unit = {

    var table: DataFrame = null
    var format = ""
    var option = Map[String, String]()
    var path = ""
    var tableName = ""
    (0 to ctx.getChildCount() - 1).foreach { tokenIndex =>
      ctx.getChild(tokenIndex) match {
        case s: FormatContext =>
          format = s.getText
        case s: ExpressionContext =>
          option += (cleanStr(s.identifier().getText) -> evaluate(cleanStr(s.STRING().getText)))
        case s: BooleanExpressionContext =>
          option += (cleanStr(s.expression().identifier().getText) -> evaluate(cleanStr(s.expression().STRING().getText)))
        case s: PathContext =>
          path = s.getText
        case s: TableNameContext =>
          //TODO
          tableName = scriptSQLExecListener.resolveTableName(s.getText)
        case _ =>
      }
    }


    if (format.startsWith("kafka")) {
      scriptSQLExecListener.addEnv("stream", "true")
      new StreamLoadAdaptor(scriptSQLExecListener, option, path, tableName, format).parse
    } else {
      new BatchLoadAdaptor(scriptSQLExecListener, option, path, tableName, format).parse
    }
    scriptSQLExecListener.setLastSelectTable(tableName)

  }
}

class BatchLoadAdaptor(scriptSQLExecListener: ScriptSQLExecListener,
                       option: Map[String, String],
                       var path: String,
                       tableName: String,
                       format: String
                      ) extends DslTool {
  def parse = {
    var table: DataFrame = null
    val reader = scriptSQLExecListener.sparkSession.read
    if(option.contains("schema")) {
      println(option("schema"))
      reader.schema(option("schema"))
    }
    reader.options(option)
    path = TemplateMerge.merge(path, scriptSQLExecListener.env().toMap)
    format match {
      case "jdbc" =>
        val dbAndTable = cleanStr(path).split("\\.")
        ScriptSQLExec.dbMapping.get(dbAndTable(0)).foreach { f =>
          reader.option(f._1, f._2)
        }
        reader.option("dbtable", dbAndTable(1))
        table = reader.format("jdbc").load()

      case "es" | "org.elasticsearch.spark.sql" =>

        val dbAndTable = cleanStr(path).split("\\.")
        ScriptSQLExec.dbMapping.get(dbAndTable(0)).foreach {
          f =>
            reader.option(f._1, f._2)
        }
        table = reader.format("org.elasticsearch.spark.sql").load(dbAndTable(1))
      case "hbase" | "org.apache.spark.sql.execution.datasources.hbase" =>
        table = reader.option("inputTableName", cleanStr(path)).format("org.apache.spark.sql.execution.datasources.hbase").load()
      case "crawlersql" =>
        table = reader.option("path", cleanStr(path)).format("org.apache.spark.sql.execution.datasources.crawlersql").load()
      case "tidb" =>
        val dbAndTable = cleanStr(path).split("\\.")
        val tidbDatabase = ScriptSQLExec.dbMapping.get(dbAndTable(0)).getOrElse("path","")
        val tiContext = PlatformManager.getRuntime.asInstanceOf[SparkRuntime].tiContext
        table =  tiContext.tidbMapTable(tidbDatabase, dbAndTable(1))
      case "kudu" =>
        //2018.08.30, load grammar add kudu data source
        val dbAndTable = cleanStr(path).split("\\.")
        ScriptSQLExec.dbMapping.get(dbAndTable(0)).foreach { f =>
          reader.option(f._1, f._2)
        }
        reader.option("kudu.table", dbAndTable(1))
        table = reader.format("org.apache.kudu.spark.kudu").load()
      case "ck" =>
        Class.forName("ru.yandex.clickhouse.ClickHouseDriver")
        val dbAndTable = cleanStr(path).split("\\.")
        ScriptSQLExec.dbMapping.get(dbAndTable(0)).foreach { f =>
          f._1 match {
            case "url" =>
              reader.option("url", s"jdbc:clickhouse://${f._2}:${ScriptSQLExec.dbMapping.get(dbAndTable(0)).getOrElse("port", "8123")}")
            case _ =>
              reader.option(f._1, f._2)
          }
        }
        reader.option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
        reader.option("dbtable", dbAndTable(1))
        table = reader.format("jdbc").load()
      case _ =>
        val owner = option.get("owner")
        if(cleanStr(path).startsWith("/")) {
          table = reader.format(format).load(withPathPrefix("", cleanStr(path)))
        } else {
          table = reader.format(format).load(withPathPrefix(scriptSQLExecListener.pathPrefix(owner), cleanStr(path)))
        }
    }

    table.createOrReplaceTempView(tableName)
  }
}

class StreamLoadAdaptor(scriptSQLExecListener: ScriptSQLExecListener,
                        option: Map[String, String],
                        var path: String,
                        tableName: String,
                        format: String
                       ) extends DslTool {
  val logger = Logger.getLogger(getClass.getName)


  def withWaterMark(table: DataFrame, option: Map[String, String]) = {
    if (option.contains("eventTimeCol")) {
      table.withWatermark(option("eventTimeCol"), option("delayThreshold"))
    } else {
      table
    }
  }

  def parse = {
    path = cleanStr(path)
    var table: DataFrame = null
    val reader = scriptSQLExecListener.sparkSession.readStream
    val startingOffsets = scriptSQLExecListener.env().get("startingOffsets").get
    startingOffsets match {
      case "earliest" | "latest" =>
        HDFSOperator.deleteDir(withPathPrefix(scriptSQLExecListener.pathPrefix(scriptSQLExecListener.env().get("owner")),  s"checkpoint/${SparkJobManager.getGroupJobInfo(scriptSQLExecListener.groupId).get.jobName}"))
        reader.option("startingOffsets", startingOffsets)
      case "checkpoint" =>
    }

    format match {
      case "kafka" =>
        val dbAndTable = cleanStr(path).split("\\.")
        ScriptSQLExec.dbMapping.get(dbAndTable(0)).foreach { f =>
          reader.option(f._1, f._2)
        }
      case _ =>
        logger.error(s"currently ss not suport source format for $format")
    }

    table = reader.options(option).format(format).load()
    table = withWaterMark(table, option)

    path = TemplateMerge.merge(path, scriptSQLExecListener.env().toMap)
    table.createOrReplaceTempView(tableName)
  }
}
