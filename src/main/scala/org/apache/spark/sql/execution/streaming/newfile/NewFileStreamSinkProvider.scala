package org.apache.spark.sql.execution.streaming.newfile

import org.apache.spark.sql.{AnalysisException, SQLContext}
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.sources.StreamSinkProvider
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.orc.OrcFileFormat
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.text.TextFileFormat


class NewFileStreamSinkProvider extends StreamSinkProvider {
  override def createSink(sqlContext: SQLContext, parameters: Map[String, String], partitionColumns: Seq[String], outputMode: OutputMode): Sink = {
    parameters.getOrElse("path", {
      throw new IllegalArgumentException("'path' is not specified")
    })
    if (outputMode != OutputMode.Append) {
      throw new AnalysisException(
        s"Data source ${getClass.getCanonicalName} does not support $outputMode output mode")
    }
    var fileFormat: FileFormat = null
    parameters.getOrElse("format", "") match {
      case "json" =>
        fileFormat = new JsonFileFormat()
      case "parquet" =>
        fileFormat = new ParquetFileFormat
      case "orc" =>
        fileFormat = new OrcFileFormat()
      case "csv" =>
        fileFormat = new CSVFileFormat()
      case "text" =>
        fileFormat = new TextFileFormat()
      case _ =>
        throw new IllegalArgumentException("'format' must be in [text,csv,json,parquet,orc]")
    }
    new NewFileStreamSink(sqlContext.sparkSession, parameters("path"), fileFormat, partitionColumns, parameters)
  }

}
