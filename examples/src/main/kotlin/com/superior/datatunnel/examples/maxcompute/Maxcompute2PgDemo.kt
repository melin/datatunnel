package com.superior.datatunnel.examples.maxcompute

import com.superior.datatunnel.core.DataTunnelExtensions
import org.apache.spark.sql.SparkSession

object Maxcompute2PgDemo {

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
            SINK("postgresql") OPTIONS (
              username = "postgres",
              password = "postgres2023",
              host = '172.18.1.56',
              port = 5432,
              databaseName = 'postgres',
              schemaName = 'public',
              tableName = 'pg_orders',
              writeMode = 'upsert',
              columns = ["*"])
        """.trimIndent()

        spark.sql(sql1)
    }
}