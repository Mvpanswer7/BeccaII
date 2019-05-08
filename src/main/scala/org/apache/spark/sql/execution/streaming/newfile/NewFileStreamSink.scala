package org.apache.spark.sql.execution.streaming.newfile

import _root_.xmatrix.common.RenderEngine
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.{FileFormat, FileFormatWriter}
import org.apache.spark.sql.execution.streaming.{FileStreamSink, FileStreamSinkLog, Sink}
import org.apache.spark.sql.{SparkSession, _}
import org.joda.time.DateTime

class NewFileStreamSink(
                         sparkSession: SparkSession,
                         _path: String,
                         fileFormat: FileFormat,
                         partitionColumnNames: Seq[String],
                         options: Map[String, String]) extends Sink with Logging {
  // 使用velocity模板引擎,方便实现复杂的模板渲染
  def evaluate(value: String, context: Map[String, AnyRef]) = {
    RenderEngine.render(value, context)
  }

  // 将路径获取改成一个方法调用，这样每次写入时，都会通过方法调用
  //从而获得一个新值
  def path = {
    evaluate(_path, Map("date" -> new DateTime()))
  }
  // 这些路径获取都需要变成方法
  private def basePath = new Path(path)

  private def logPath = new Path(basePath, FileStreamSink.metadataDir)

  private def fileLog =
    new FileStreamSinkLog(FileStreamSinkLog.VERSION, sparkSession, logPath.toUri.toString)

  private val hadoopConf = sparkSession.sessionState.newHadoopConf()

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    if (batchId <= fileLog.getLatest().map(_._1).getOrElse(-1L)) {
      logInfo(s"Skipping already committed batch $batchId")
    } else {
      val committer = new NewStreamFileCommitProtocol(path, options.getOrElse("min.bytes.file", "1").toInt)
      committer.setupManifestOptions(fileLog,batchId, hadoopConf)

      FileFormatWriter.write(
        sparkSession,
        data.queryExecution.sparkPlan,
        fileFormat,
        committer,
        FileFormatWriter.OutputSpec(path, Map.empty,data.schema.toAttributes),
        hadoopConf,
        Seq.empty,
        None,
        Seq.empty,
        options
      )
    }
  }

  override def toString: String = s"FileSink[$path]"
}

