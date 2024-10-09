package com.superior.datatunnel.plugin.kafka.util

import com.superior.datatunnel.common.enums.OutputMode
import com.superior.datatunnel.common.util.FsUtils
import org.apache.commons.lang3.StringUtils
import org.apache.iceberg.CatalogProperties
import org.apache.iceberg.hive.HiveCatalog
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.streaming.Trigger

import java.util
import java.util.concurrent.TimeUnit

/**
 * https://www.dremio.com/blog/row-level-changes-on-the-lakehouse-copy-on-write-vs-merge-on-read-in-apache-iceberg/
 * https://medium.com/@geekfrosty/copy-on-write-or-merge-on-read-what-when-and-how-64c27061ad56 多数据源简单适配
 * https://www.dremio.com/blog/compaction-in-apache-iceberg-fine-tuning-your-iceberg-tables-data-files/?source=post_page-----a653545de087--------------------------------
 */
object IcebergUtils extends Logging {

  private val PARTITION_COL_NAME = "ds";

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
      outputMode: OutputMode,
      getMergeKeys: String,
      querySql: String
  ): Unit = {
    val catalogTable = spark.sessionState.catalog.getTableMetadata(identifier)

    FsUtils.mkDir(spark, checkpointLocation)

    val streamingInput = spark.sql(querySql)
    val writer = streamingInput.writeStream
      .trigger(Trigger.ProcessingTime(triggerProcessingTime, TimeUnit.SECONDS))
      .format("delta")
      .option("checkpointLocation", checkpointLocation)

    val catalog = new HiveCatalog
    val conf = spark.sparkContext.hadoopConfiguration
    catalog.setConf(conf)
    val properties: util.Map[String, String] = new util.HashMap[String, String]
    val uris: String = conf.get("hive.metastore.uris")
    properties.put(CatalogProperties.URI, uris)
    catalog.initialize("hive", properties)
    val icebergTable =
      catalog.loadTable(org.apache.iceberg.catalog.TableIdentifier.of(identifier.database.get, identifier.table))

    if (icebergTable.spec().isPartitioned) {
      writer.option("fanout-enabled", "true")
      writer.toTable(identifier.toString())
    } else {
      writer.option("path", catalogTable.location.toString)
    }

    catalog.close()

    if (StringUtils.isBlank(getMergeKeys)) {
      writer
        .outputMode(outputMode.getName)
        .start()
        .awaitTermination()
    } else {
      val foreachBatchFn = new ForeachBatchFn(getMergeKeys, identifier)
      writer
        .foreachBatch(foreachBatchFn)
        .outputMode("update")
        .start()
        .awaitTermination()
    }
  }
}
