package com.superior.datatunnel.plugin.maxcompute

import org.apache.spark.sql.SparkSession

/**
  * @author renxiang
  * @date 2021-12-20
  */
object DataReaderTest {

  val ODPS_DATA_SOURCE = "org.apache.spark.sql.odps.datasource.DefaultSource"
  val ODPS_ENDPOINT = "http://service.cn.maxcompute.aliyun.com/api"

  def main(args: Array[String]): Unit = {
    val odpsProject = "aloudata"
    val odpsAkId = "0rHycgWdKrPkIZpO"
    val odpsAkKey = "eAQR7Y4nJTViOarwiDRLHVJl78qs8M"
    val odpsTable = "orders"

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("odps-datasource-reader")
      .getOrCreate()

    import spark._

    val df = spark.read.format(ODPS_DATA_SOURCE)
      .option("spark.hadoop.odps.project.name", odpsProject)
      .option("spark.hadoop.odps.access.id", odpsAkId)
      .option("spark.hadoop.odps.access.key", odpsAkKey)
      .option("spark.hadoop.odps.end.point", ODPS_ENDPOINT)
      .option("spark.hadoop.odps.table.name", odpsTable)
      .load()

    df.createOrReplaceTempView("odps_table")

    println("select * from odps_table")
    val dfFullScan = sql("select * from odps_table")
    println(dfFullScan.count)
    dfFullScan.show(20)
  }
}
