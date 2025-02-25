package com.superior.datatunnel.plugin.kafka.util

import org.apache.commons.lang3.StringUtils
import org.apache.spark.api.java.function.VoidFunction2
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.TableIdentifier

class DeltaForeachBatchFn(val mergeColumns: String, val identifier: TableIdentifier, compactionEnabled: Boolean)
    extends VoidFunction2[DataFrame, java.lang.Long]
    with Serializable {
  override def call(microBatchOutputDF: DataFrame, batchId: java.lang.Long): Unit = {
    val spark = microBatchOutputDF.sparkSession

    if (StringUtils.isNotBlank(mergeColumns)) {
      val keys = mergeColumns.split(",")
      val duplicateDF = microBatchOutputDF.dropDuplicates(keys)
      duplicateDF.createOrReplaceTempView("updates")
      val keyCondition = keys.map(key => s"s.${key} = t.${key}").mkString(" and ")
      val mergeInto =
        s"""
      MERGE INTO ${identifier.toString()} t
      USING updates s
      ON ${keyCondition}
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *
    """
      spark.sql(mergeInto)
    } else {
      microBatchOutputDF.write.insertInto(identifier.toString())
    }
  }
}
