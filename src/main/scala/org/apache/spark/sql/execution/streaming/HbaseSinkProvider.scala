package org.apache.spark.sql.execution.streaming

import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, SQLContext}

class HBaseSinkProvider extends StreamSinkProvider with DataSourceRegister {
  override def createSink(
                  sqlContext: SQLContext,
                  parameters: Map[String, String],
                  partitionColumns: Seq[String],
                  outputMode: OutputMode
                  ): Sink = {
    new HBaseSink(sqlContext,parameters,partitionColumns,outputMode)
  }

  def shortName(): String = "hbase"
}

case class HBaseSink (
                       sqlContext: SQLContext,
                       parameters: Map[String, String],
                       partitionColumns: Seq[String],
                       outputMode: OutputMode) extends Sink {

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    val writer = data.sparkSession.createDataFrame(
      data.sparkSession.sparkContext.parallelize(data.collect()),data.schema).write
    parameters foreach {
      f=>
        writer.option(f._1, f._2)
    }
    writer.option("outputTableName", parameters("path")).format(
      "org.apache.spark.sql.execution.datasources.hbase").save()
  }

}
