package org.apache.spark.sql.execution.streaming

/**
  * Created by iodone on {18-7-20}.
  */

import org.apache.spark.sql.{AnalysisException, SQLContext}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.streaming.{FileStreamSink, Sink}
import org.apache.spark.sql.sources.StreamSinkProvider
import org.apache.spark.sql.streaming.OutputMode

//class JDBCBatchSinkProvider extends StreamSinkProvider {
//  override def createSink(sqlContext: SQLContext, parameters: Map[String, String], partitionColumns: Seq[String], outputMode: OutputMode): Sink = {
//  }
//}
