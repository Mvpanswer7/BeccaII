package org.apache.spark.sql.execution.streaming.newfile

import java.util.UUID

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.{JobContext, TaskAttemptContext}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage
import org.apache.spark.sql.execution.streaming.{FileStreamSinkLog, SinkFileStatus}

import scala.collection.mutable.ArrayBuffer

class NewStreamFileCommitProtocol(path: String, minSize: Int)
  extends FileCommitProtocol with Serializable with Logging {

  // Track the list of files added by a task, only used on the executors.
  @transient private var addedFiles: ArrayBuffer[String] = _

  @transient private var fileLog: FileStreamSinkLog = _
  @transient private var hadoopConfig: Configuration = _
  private var batchId: Long = _
  private var taskFielMap: Map[Int, String] = _

  /**
    * Sets up the manifest log output and the batch id for this job.
    * Must be called before any other function.
    */
  def setupManifestOptions(_fileLog: FileStreamSinkLog, _batchId: Long, _hadoopConfig: Configuration): Unit = {
    this.fileLog = _fileLog
    this.batchId = _batchId
    this.hadoopConfig = _hadoopConfig
    //getLastTaskFileMap()
  }


  def deleteEmptyFile(path: Path): Unit = {
    var fs: FileSystem = null
    try {
      fs = FileSystem.get(hadoopConfig)
      if (fs.exists(path) && fs.isFile(path)) {
        fs.delete(path, false)
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally {
      if (!fs.equals(null)) {
        fs.close()
      }
      fs = null
    }
  }

  def getLastTaskFileMap() {
    require(fileLog != null, "setupManifestOptions must be called before this function")
    this.taskFielMap = Map.empty
    fileLog.getLatest() match {
      case None =>
      case Some(batchIdLog) =>
        batchIdLog._2.foreach(f =>
        {
          val filePath = f.path
          val partitionNum = "\\-\\d{5}\\-".r.findFirstIn(filePath) match {
            case None => -1
            case Some(s) =>
              s.replaceAll("\\-", "").toInt
          }
          if (partitionNum >= 0 && f.size < f.blockSize) {
            this.taskFielMap += (partitionNum -> filePath)
          }

        }
        )

    }
  }

  override def setupJob(jobContext: JobContext): Unit = {
    require(fileLog != null, "setupManifestOptions must be called before this function")
    // Do nothing
  }

  override def commitJob(jobContext: JobContext, taskCommits: Seq[TaskCommitMessage]): Unit = {
    require(fileLog != null, "setupManifestOptions must be called before this function")
    val fileStatuses = taskCommits.flatMap(_.obj.asInstanceOf[Seq[SinkFileStatus]]).toArray

    if (fileLog.add(batchId, fileStatuses)) {
      logInfo(s"Committed batch $batchId")
    } else {
      throw new IllegalStateException(s"Race while writing batch $batchId")
    }
  }

  override def abortJob(jobContext: JobContext): Unit = {
    require(fileLog != null, "setupManifestOptions must be called before this function")
    // Do nothing
  }

  override def setupTask(taskContext: TaskAttemptContext): Unit = {
    addedFiles = new ArrayBuffer[String]
  }

  override def newTaskTempFile(
                                taskContext: TaskAttemptContext, dir: Option[String], ext: String): String = {
    // The file name looks like part-r-00000-2dd664f9-d2c4-4ffe-878f-c6c70c1fb0cb_00003.gz.parquet
    // Note that %05d does not truncate the split number, so if we have more than 100000 tasks,
    // the file name is fine and won't overflow.
    val split = taskContext.getTaskAttemptID.getTaskID.getId
    val uuid = UUID.randomUUID.toString
    /*
    val filename = taskFielMap.get(split) match {
      case Some(f) =>
        f
      case None =>
        f"part-$split%05d-$uuid$ext"
    }
     */
    val filename = f"part-$split%05d-$uuid$ext"
    val file = dir.map { d =>
      new Path(new Path(path, d), filename).toString
    }.getOrElse {
      new Path(path, filename).toString
    }

    addedFiles += file
    file
  }

  override def newTaskTempFileAbsPath(
                                       taskContext: TaskAttemptContext, absoluteDir: String, ext: String): String = {
    throw new UnsupportedOperationException(
      s"$this does not support adding files with an absolute path")
  }

  override def commitTask(taskContext: TaskAttemptContext): TaskCommitMessage = {
    if (addedFiles.nonEmpty) {
      val fs = new Path(addedFiles.head).getFileSystem(taskContext.getConfiguration)
      val statuses: Seq[SinkFileStatus] =
        addedFiles.map(f => SinkFileStatus(fs.getFileStatus(new Path(f))))
      new TaskCommitMessage(statuses)
    } else {
      new TaskCommitMessage(Seq.empty[SinkFileStatus])
    }
  }

  override def abortTask(taskContext: TaskAttemptContext): Unit = {
    // Do nothing
    // TODO: we can also try delete the addedFiles as a best-effort cleanup.
  }

  override def onTaskCommit(taskCommit: TaskCommitMessage): Unit = {
    taskCommit.obj match {
      case stats: scala.collection.mutable.ArrayBuffer[SinkFileStatus]  =>
        stats.filter(stat => stat.size < minSize)
        .foreach(stat =>
          deleteEmptyFile(new Path(stat.path))
        )
      case scala.collection.immutable.Nil =>
        println(s"${this.getClass.toGenericString} task committed nothing")
      case _ =>
        println(taskCommit.obj.getClass.toGenericString)
    }
  }
}
