package com.superior.datatunnel.distcp.objects

import com.superior.datatunnel.distcp.utils.FileUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator

import scala.collection.JavaConverters._

class Accumulators(sparkSession: SparkSession) extends Serializable {

  def handleResult(result: DistCPResult): Unit = result match {
    case DeleteResult(
          _,
          DeleteActionResult.SkippedDoesNotExists |
          DeleteActionResult.SkippedDryRun
        ) =>
      deleteOperationsSkipped.add(1)
    case DeleteResult(_, DeleteActionResult.Deleted) =>
      deleteOperationsSuccessful.add(1)
    case DeleteResult(_, DeleteActionResult.Failed(e)) =>
      deleteOperationsSkipped.add(1)
      deleteOperationsFailed.add(1)
      exceptionCount.add(e)
    case DirectoryCopyResult(
          _,
          _,
          CopyActionResult.SkippedAlreadyExists | CopyActionResult.SkippedDryRun
        ) =>
      foldersSkipped.add(1)
    case DirectoryCopyResult(_, _, CopyActionResult.Created) =>
      foldersCreated.add(1)
    case DirectoryCopyResult(_, _, CopyActionResult.Failed(e)) =>
      foldersFailed.add(1)
      foldersSkipped.add(1)
      exceptionCount.add(e)
    case FileCopyResult(
          _,
          _,
          l,
          CopyActionResult.SkippedAlreadyExists |
          CopyActionResult.SkippedIdenticalFileAlreadyExists |
          CopyActionResult.SkippedDryRun
        ) =>
      filesSkipped.add(1)
      bytesSkipped.add(l)
    case FileCopyResult(_, _, l, CopyActionResult.Copied) =>
      filesCopied.add(1)
      bytesCopied.add(l)
    case FileCopyResult(_, _, l, CopyActionResult.OverwrittenOrUpdated) =>
      filesCopied.add(1)
      bytesCopied.add(l)
      filesUpdatedOrOverwritten.add(1)
    case FileCopyResult(_, _, l, CopyActionResult.Failed(e)) =>
      filesFailed.add(1)
      exceptionCount.add(e)
      filesSkipped.add(1)
      bytesSkipped.add(l)
  }

  def getOutputText: String = {
    val intFormatter = java.text.NumberFormat.getIntegerInstance
    s"""--Raw data--
       |\tData copied: ${FileUtils.byteCountToDisplaySize(bytesCopied.value)}
       |\tData skipped (already existing files, dry-run and failures): ${FileUtils
      .byteCountToDisplaySize(bytesSkipped.value)}
       |--Files--
       |\tFiles copied (new files and overwritten/updated files): ${intFormatter
      .format(filesCopied.value)}
       |\tFiles overwritten/updated: ${intFormatter.format(
      filesUpdatedOrOverwritten.value
    )}
       |\tSkipped files for copying (already existing files, dry-run and failures): ${intFormatter
      .format(filesSkipped.value)}
       |\tFailed files during copy: ${intFormatter.format(filesFailed.value)}
       |--Folders--
       |\tFolders created: ${intFormatter.format(foldersCreated.value)}
       |\tSkipped folder creates (already existing folders, dry-run and failures): ${intFormatter
      .format(foldersSkipped.value)}
       |\tFailed folder creates: ${intFormatter.format(foldersFailed.value)}
       |--Deletes--
       |\tSuccessful delete operations: ${intFormatter.format(
      deleteOperationsSuccessful.value
    )}
       |\tSkipped delete operations (files/folders already missing, dry-run and failures): ${intFormatter
      .format(deleteOperationsSkipped.value)}
       |\tFailed delete operations: ${intFormatter.format(
      deleteOperationsFailed.value
    )}
       |--Exception counts--
       |\t""".stripMargin ++
      exceptionCount.value.asScala.toSeq
        .sortWith { case ((_, v1), (_, v2)) => v1 > v2 }
        .map { case (k, v) => s"$k: ${intFormatter.format(v)}" }
        .mkString("\n")
  }

  val bytesCopied: LongAccumulator =
    sparkSession.sparkContext.longAccumulator("BytesCopied")
  val bytesSkipped: LongAccumulator = sparkSession.sparkContext.longAccumulator(
    "BytesSkipped"
  ) // Already exists, dryrun and failure

  val foldersCreated: LongAccumulator =
    sparkSession.sparkContext.longAccumulator("FoldersCreated")
  val foldersSkipped: LongAccumulator =
    sparkSession.sparkContext.longAccumulator("FoldersSkipped")
  val foldersFailed: LongAccumulator =
    sparkSession.sparkContext.longAccumulator("FoldersFailed")

  val filesCopied: LongAccumulator =
    sparkSession.sparkContext.longAccumulator("FilesCopied")
  val filesSkipped: LongAccumulator = sparkSession.sparkContext.longAccumulator(
    "FilesSkipped"
  ) // Already exists, dryrun and failure
  val filesFailed: LongAccumulator =
    sparkSession.sparkContext.longAccumulator("FilesFailed")
  val filesUpdatedOrOverwritten: LongAccumulator =
    sparkSession.sparkContext.longAccumulator("FilesUpdatedOrOverwritten")

  val deleteOperationsSuccessful: LongAccumulator =
    sparkSession.sparkContext.longAccumulator("DeleteOperationsSuccessful")
  val deleteOperationsSkipped: LongAccumulator =
    sparkSession.sparkContext.longAccumulator(
      "DeleteOperationsSkipped"
    ) // Already exists, dryrun and failure
  val deleteOperationsFailed: LongAccumulator =
    sparkSession.sparkContext.longAccumulator("DeleteOperationsFailed")

  val exceptionCount: ExceptionCountAccumulator = new ExceptionCountAccumulator
  sparkSession.sparkContext.register(exceptionCount, "ExceptionCount")
}
