package com.dataworker.datax.core

import org.apache.spark.sql.SparkSession

/**
 * huaixin 2021/12/27 4:27 PM
 */
object Test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .config("spark.sql.extensions", "com.dataworker.datax.core.DataxExtensions")
      .getOrCreate()

    val sql =
      """
        |datax reader("jdbc") options(datasourceCode='UsoiB9rP', databaseName='dataworks', tableName='dc_datax_datasource', column=["*"])
        |    writer("hive") options(tableName='hive_datax_datasource', writeMode='overwrite', column=["*"])
        |""".stripMargin
    spark.sql(sql)

  }
}
