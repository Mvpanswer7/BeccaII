package org.apache.spark.sql.execution.streaming

import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, SQLContext}
import xmatrix.core.platform.{PlatformManager, SparkRuntime}
import xmatrix.dsl.ScriptSQLExec
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.kudu.client.CreateTableOptions

import collection.JavaConverters._

class KuduSinkProvider extends StreamSinkProvider with DataSourceRegister {
  override def createSink(sqlContext: SQLContext, parameters: Map[String, String], partitionColumns: Seq[String], outputMode: OutputMode): Sink = {
    new KuduSink(sqlContext,parameters,partitionColumns,outputMode)
  }

  override def shortName(): String = "kudu"
}

case class KuduSink (
                       sqlContext: SQLContext,
                       parameters: Map[String, String],
                       partitionColumns: Seq[String],
                       outputMode: OutputMode) extends Sink {

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    val oldDF = data.sparkSession.createDataFrame(
      data.sparkSession.sparkContext.parallelize(data.collect()), data.schema)

    val dbAndTable = parameters("path").split("\\.")
    val kuduConfig = ScriptSQLExec.dbMapping.get(dbAndTable(0))
    val final_path = dbAndTable(1)
    val kc = new KuduContext(kuduConfig("kudu.master"), PlatformManager.getRuntime.asInstanceOf[SparkRuntime].sparkSession.sparkContext)
    if (!kc.tableExists(final_path)) {
      val newSchema = StructType(oldDF.schema.map {
        case StructField(c, t, _, m) if c.equals(parameters("kudu.primaryKey")) => StructField(c, t, nullable=false, m)
        case y: StructField => y
      })
      kc.createTable(
        final_path, newSchema, Seq(parameters("kudu.primaryKey")),
        new CreateTableOptions()
          .setNumReplicas(parameters("kudu.replicas").toInt)
          .addHashPartitions(
            List(parameters("kudu.hashPartitionsColumns")).asJava,
            parameters("kudu.hashPartitionsBuckets").toInt))
    }
    val writer = oldDF.write
    parameters foreach {
      f=>
        writer.option(f._1, f._2)
    }
    writer.option("kudu.table", final_path).mode("append")
    writer.format("org.apache.kudu.spark.kudu").save()
  }
}
