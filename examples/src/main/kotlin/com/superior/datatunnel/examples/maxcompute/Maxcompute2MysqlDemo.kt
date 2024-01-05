package com.superior.datatunnel.examples.maxcompute

import com.superior.datatunnel.core.DataTunnelExtensions
import org.apache.spark.sql.SparkSession

object Maxcompute2MysqlDemo {

    @JvmStatic
    fun main(args: Array<String>) {
        System.setProperty("HADOOP_USER_NAME", "hdfs");

        val spark = SparkSession
            .builder()
            .master("local")
            .enableHiveSupport()
            .appName("Datatunnel spark example")
            .config("spark.sql.extensions", DataTunnelExtensions::class.java.name)
            .getOrCreate()

        val sql1 = """
            DATATUNNEL SOURCE("maxcompute") OPTIONS (
              projectName = "superior",
              tableName = "ods_orders_pt",
              accessKeyId = 'LTAI5tNvrRiDkqnAWuP9JLs7',
              secretAccessKey = 'YVX4Lo2zor5TlQhFn86oLhpY25Azdx',
              endpoint='http://service.us-east-1.maxcompute.aliyun.com/api',
              columns = ["*"],
              partitionSpec = "pt>'20230718' and pt<='20230719'"
            )
            SINK("mysql") OPTIONS (
              username = "root",
              password = "root2023",
              host = '172.18.5.44',
              port = 3306,
              schemaName = 'demos',
              tableName = 'pg_orders',
              writeMode = 'BULKINSERT',
              columns = ["*"])
        """.trimIndent()

        spark.sql(sql1)
    }
}