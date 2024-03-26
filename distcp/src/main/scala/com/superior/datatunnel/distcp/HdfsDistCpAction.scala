package com.superior.datatunnel.distcp

import com.superior.datatunnel.api.model.DistCpOption
import com.superior.datatunnel.api.{DistCpAction, DistCpContext}
import com.superior.datatunnel.distcp.SparkDistCP.{doCopy, doDelete}
import com.superior.datatunnel.distcp.objects._
import com.superior.datatunnel.distcp.utils.PathUtils
import org.apache.hadoop.fs.Path
import com.superior.datatunnel.distcp.utils._
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._
import scala.util.matching.Regex

class HdfsDistCpAction extends DistCpAction with Logging {

  override def run(context: DistCpContext): Unit = {
    val sparkSession = context.getSparkSession
    val option: DistCpOption = context.getOption

    val qualifiedSourcePaths = option.getSrcPaths.map(path =>
      PathUtils
        .pathToQualifiedPath(sparkSession.sparkContext.hadoopConfiguration, new Path(path))
    )

    val qualifiedDestinationPath = PathUtils.pathToQualifiedPath(
      sparkSession.sparkContext.hadoopConfiguration,
      new Path(option.getDestPath)
    )

    val filterNotRegs: List[Regex] = if (option.getFilterNot == null) List.empty else
      option.getFilterNot.asScala.map(new Regex(_)).toList

    val sourceRDD = FileListUtils.getSourceFiles(
      sparkSession.sparkContext,
      qualifiedSourcePaths.map(_.toUri),
      qualifiedDestinationPath.toUri,
      option.updateOverwritePathBehaviour,
      option.getNumListstatusThreads,
      filterNotRegs
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

    logInfo("SparkDistCP Run Statistics\n" + accumulators.getOutputText)
  }
}
