package com.superior.datatunnel.plugin.kafka.util

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.superior.datatunnel.common.util.FsUtils
import com.superior.datatunnel.plugin.kafka.DatalakeDatatunnelSinkOption
import org.apache.commons.lang3.StringUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.streaming.Trigger

import java.util
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._

/** https://www.dremio.com/blog/row-level-changes-on-the-lakehouse-copy-on-write-vs-merge-on-read-in-apache-iceberg/
  * https://medium.com/@geekfrosty/copy-on-write-or-merge-on-read-what-when-and-how-64c27061ad56 多数据源简单适配
  * https://www.dremio.com/blog/compaction-in-apache-iceberg-fine-tuning-your-iceberg-tables-data-files/?source=post_page-----a653545de087--------------------------------
  */
object IcebergUtils extends Logging {

  def isIcebergTable(identifier: TableIdentifier): Boolean = {
    val table = SparkSession.active.sessionState.catalog.getTableMetadata(identifier)
    val tableType = table.properties.get("table_type")
    tableType.isDefined && tableType.get.equalsIgnoreCase("iceberg")
  }

  /** delta insert select 操作
    */
  def writeStreamSelectAdapter(
      spark: SparkSession,
      identifier: TableIdentifier,
      checkpointLocation: String,
      triggerProcessingTime: Long,
      sinkOption: DatalakeDatatunnelSinkOption,
      querySql: String
  ): Unit = {
    FsUtils.mkDir(spark, checkpointLocation)

    val streamingInput = spark.sql(querySql)
    val outputMode = sinkOption.getOutputMode
    val writer = streamingInput.writeStream
      .trigger(Trigger.ProcessingTime(triggerProcessingTime, TimeUnit.SECONDS))
      .format("iceberg")
      .option("checkpointLocation", checkpointLocation)
      .outputMode(outputMode.getName)
      .options(sinkOption.getProperties)

    var mergeColumns = sinkOption.getMergeColumns()
    var partitionColumnNames = sinkOption.getPartitionColumnNames()

    if (StringUtils.isBlank(partitionColumnNames)) {
      val catalogTable = spark.sessionState.catalog.getTableMetadata(identifier)
      catalogTable.properties
        .get("default-partition-spec")
        .map(json => {
          val mapper = new ObjectMapper()
          val typeRef = new TypeReference[util.LinkedHashMap[String, Object]]() {}
          val map = mapper.readValue(json, typeRef)
          val parts = map.get("fields").asInstanceOf[util.ArrayList[util.LinkedHashMap[String, Object]]]
          partitionColumnNames = parts.asScala.map(part => part.get("name")).mkString(",")
          logInfo("auto partitionColumnNames:" + partitionColumnNames)
        })
    }

    if (StringUtils.isNotBlank(partitionColumnNames)) {
      writer.option("fanout-enabled", "true")
    }

    if (StringUtils.isBlank(mergeColumns)) {
      if (StringUtils.isNotBlank(partitionColumnNames)) {
        writer.partitionBy(StringUtils.split(partitionColumnNames, ","): _*)
      }
    } else {
      if (StringUtils.isNotBlank(partitionColumnNames)) {
        mergeColumns = mergeColumns + "," + partitionColumnNames
      }
    }

    val foreachBatchFn = new IcebergForeachBatchFn(mergeColumns, identifier, sinkOption.isCompactionEnabled)
    writer
      .foreachBatch(foreachBatchFn)
      .start()
      .awaitTermination()
  }
}
