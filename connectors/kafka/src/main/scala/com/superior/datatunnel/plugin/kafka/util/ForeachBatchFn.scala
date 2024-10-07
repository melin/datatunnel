package com.superior.datatunnel.plugin.kafka.util

import org.apache.spark.api.java.function.VoidFunction2
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.TableIdentifier

class ForeachBatchFn(val getMergeKeys: String, val identifier: TableIdentifier)
    extends VoidFunction2[DataFrame, java.lang.Long]
    with Serializable {
  override def call(microBatchOutputDF: DataFrame, batchId: java.lang.Long): Unit = {
    val keys = getMergeKeys.split(",")
    val duplicateDF = microBatchOutputDF.dropDuplicates(keys)
    duplicateDF.createOrReplaceTempView("updates")
    val keyCondition = keys.map(key => s"s.${key} = t.${key}").mkString(" and ")
    val mergeInto = s"""
      MERGE INTO ${identifier.toString()} t
      USING updates s
      ON ${keyCondition}
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *
    """
    microBatchOutputDF.sparkSession.sql(mergeInto)
  }
}
