package com.superior.datatunnel.distcp

import com.superior.datatunnel.api.model.DistCpOption
import com.superior.datatunnel.api.{DistCpAction, DistCpContext}
import com.superior.datatunnel.distcp.HdfsDistCpAction.{doCopy, doDelete}
import com.superior.datatunnel.distcp.objects._
import com.superior.datatunnel.distcp.utils.PathUtils
import org.apache.hadoop.fs.Path
import com.superior.datatunnel.distcp.utils._
import io.github.melin.jobserver.spark.api.LogUtils
import org.apache.spark.{HashPartitioner, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD

import java.net.URI
import scala.util.matching.Regex

class HdfsDistCpAction extends DistCpAction with Logging {

  override def run(context: DistCpContext): Unit = {
    val sparkSession = context.getSparkSession
    val option: DistCpOption = context.getOption

    val qualifiedSourcePaths = option.getSrcPaths.map(path =>
      PathUtils
        .pathToQualifiedPath(
          sparkSession.sparkContext.hadoopConfiguration,
          new Path(path)
        )
    )

    val qualifiedDestinationPath = PathUtils.pathToQualifiedPath(
      sparkSession.sparkContext.hadoopConfiguration,
      new Path(option.getDestPath)
    )

    val includesRegex: List[Regex] =
      if (option.getIncludes == null) List.empty
      else
        option.getIncludes.toList.map(new Regex(_))

    val excludesRegex: List[Regex] =
      if (option.getExcludes == null) List.empty
      else
        option.getExcludes.toList.map(new Regex(_))

    val sourceRDD = FileListUtils.getSourceFiles(
      sparkSession.sparkContext,
      qualifiedSourcePaths.map(_.toUri),
      qualifiedDestinationPath.toUri,
      option.updateOverwritePathBehaviour,
      option.getNumListstatusThreads,
      includesRegex,
      excludesRegex
    )

    val destinationRDD = FileListUtils.getDestinationFiles(
      sparkSession.sparkContext,
      qualifiedDestinationPath,
      option
    )

    val joined = sourceRDD.fullOuterJoin(destinationRDD)

    val toCopy = joined.collect { case (_, (Some(s), _)) => s }

    val accumulators = new Accumulators(sparkSession)

    val copyResult: RDD[DistCPResult] = doCopy(toCopy, accumulators, option)

    val deleteResult: RDD[DistCPResult] = {
      if (option.isDelete) {
        val toDelete = joined.collect { case (d, (None, _)) => d }
        doDelete(toDelete, accumulators, option)
      } else {
        sparkSession.sparkContext.emptyRDD[DistCPResult]
      }
    }

    val allResults = copyResult union deleteResult
    allResults.foreach(_ => ())

    val outputText = accumulators.getOutputText
    LogUtils.info(s"Spark DistCP Run Statistics:\n${outputText}")
  }
}

object HdfsDistCpAction extends Logging {

  type KeyedCopyDefinition = (URI, CopyDefinitionWithDependencies)

  /** Perform the copy portion of the DistCP
    */
  private[distcp] def doCopy(
      sourceRDD: RDD[CopyDefinitionWithDependencies],
      accumulators: Accumulators,
      options: DistCpOption
  ): RDD[DistCPResult] = {

    val serConfig = new ConfigSerDeser(
      sourceRDD.sparkContext.hadoopConfiguration
    )
    batchAndPartitionFiles(
      sourceRDD,
      options.getMaxFilesPerTask,
      options.getMaxBytesPerTask
    )
      .mapPartitions { iterator =>
        val hadoopConfiguration = serConfig.get()
        val attemptID = TaskContext.get().taskAttemptId()
        val fsCache = new FileSystemObjectCacher(hadoopConfiguration)

        iterator
          .flatMap(_._2.getAllCopyDefinitions)
          .collectMapWithEmptyCollection(
            (d, z) => z.contains(d),
            d => {
              val r = CopyUtils.handleCopy(
                fsCache.getOrCreate(d.source.uri),
                fsCache.getOrCreate(d.destination),
                d,
                options,
                attemptID
              )
              accumulators.handleResult(r)
              r
            }
          )
      }
  }

  /** Perform the delete from destination portion of the DistCP
    */
  private[distcp] def doDelete(
      destRDD: RDD[URI],
      accumulators: Accumulators,
      options: DistCpOption
  ): RDD[DistCPResult] = {
    val serConfig = new ConfigSerDeser(destRDD.sparkContext.hadoopConfiguration)
    val count = destRDD.count()
    destRDD
      .repartition((count / options.getMaxFilesPerTask).toInt.max(1))
      .mapPartitions { iterator =>
        val hadoopConfiguration = serConfig.get()
        val fsCache = new FileSystemObjectCacher(hadoopConfiguration)
        iterator
          .collectMapWithEmptyCollection(
            (d, z) => z.exists(p => PathUtils.uriIsChild(p, d)),
            d => {
              val r = CopyUtils.handleDelete(fsCache.getOrCreate(d), d, options)
              accumulators.handleResult(r)
              r
            }
          )
      }
  }

  /** DistCP helper implicits on iterators
    */
  private[distcp] implicit class DistCPIteratorImplicit[B](
      iterator: Iterator[B]
  ) {

    /** Scan over an iterator, mapping as we go with `action`, but making a
      * decision on which objects to actually keep using a set of what objects
      * have been seen and the `skip` function. Similar to a combining `collect`
      * and `foldLeft`.
      *
      * @param skip
      *   Should a mapped version of this element not be included in the output
      * @param action
      *   Function to map the element
      * @return
      *   An iterator
      */
    def collectMapWithEmptyCollection(
        skip: (B, Set[B]) => Boolean,
        action: B => DistCPResult
    ): Iterator[DistCPResult] = {

      iterator
        .scanLeft((Set.empty[B], None: Option[DistCPResult])) {
          case ((z, _), d) if skip(d, z) => (z, None)
          case ((z, _), d) =>
            (z + d, Some(action(d)))
        }
        .collect { case (_, Some(r)) => r }

    }

  }

  /** Batch the given RDD into groups of files depending on
    * [[SparkDistCPOptions.maxFilesPerTask]] and
    * [[SparkDistCPOptions.maxBytesPerTask]] and repartition the RDD so files in
    * the same batches are in the same partitions
    */
  private[distcp] def batchAndPartitionFiles(
      rdd: RDD[CopyDefinitionWithDependencies],
      maxFilesPerTask: Int,
      maxBytesPerTask: Long
  ): RDD[((Int, Int), CopyDefinitionWithDependencies)] = {
    val partitioner =
      rdd.partitioner.getOrElse(new HashPartitioner(rdd.partitions.length))
    val sorted = rdd
      .map(v => (v.source.uri.toString, v))
      .repartitionAndSortWithinPartitions(partitioner)
      .map(_._2)
    val batched = sorted.mapPartitionsWithIndex(
      generateBatchedFileKeys(maxFilesPerTask, maxBytesPerTask)
    ) // sorted

    batched.partitionBy(CopyPartitioner(batched))
  }

  /** Key the RDD within partitions based on batches of files based on
    * [[SparkDistCPOptions.maxFilesPerTask]] and
    * [[SparkDistCPOptions.maxBytesPerTask]] thresholds
    */
  private[distcp] def generateBatchedFileKeys(
      maxFilesPerTask: Int,
      maxBytesPerTask: Long
  ): (Int, Iterator[CopyDefinitionWithDependencies]) => Iterator[
    ((Int, Int), CopyDefinitionWithDependencies)
  ] = { (partition, iterator) =>
    iterator
      .scanLeft[(Int, Int, Long, CopyDefinitionWithDependencies)](
        0,
        0,
        0,
        null
      ) { case ((index, count, bytes, _), definition) =>
        val newCount = count + 1
        val newBytes = bytes + definition.source.getLen
        if (newCount > maxFilesPerTask || newBytes > maxBytesPerTask) {
          (index + 1, 1, definition.source.getLen, definition)
        } else {
          (index, newCount, newBytes, definition)
        }
      }
      .drop(1)
      .map { case (index, _, _, file) => ((partition, index), file) }
  }

}
