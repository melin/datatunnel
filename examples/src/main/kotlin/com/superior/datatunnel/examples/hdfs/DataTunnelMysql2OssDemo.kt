package com.superior.datatunnel.examples.hdfs

import com.superior.datatunnel.core.DataTunnelExtensions
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hudi.HoodieSparkSessionExtension

object DataTunnelMysql2OssDemo {
    @JvmStatic
    fun main(args: Array<String>) {
        System.setProperty("HADOOP_USER_NAME", "hdfs");

        val spark = SparkSession
            .builder()
            .master("local")
            .enableHiveSupport()
            .appName("Datatunnel spark example")
            .config("spark.sql.extensions", DataTunnelExtensions::class.java.name)
            .config("spark.hadoop.fs.oss.endpoint", "oss-cn-hangzhou.aliyuncs.com")
            .config("spark.hadoop.fs.oss.accessKeyId", "xxx")
            .config("spark.hadoop.fs.oss.accessKeySecret", "xxx")
            .config("spark.hadoop.fs.oss.attempts.maximum", "3")
            .config("spark.hadoop.fs.oss.connection.timeout", "10000")
            .config("spark.hadoop.fs.oss.connection.establish.timeout", "10000")
            .config("spark.hadoop.fs.oss.impl", "org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem")

            .config("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
            .getOrCreate()

        val sql = """
            DATATUNNEL SOURCE("mysql") OPTIONS (
              username = "root",
              password = "root2023",
              host = '172.18.5.44',
              port = 3306,
              databaseName = 'demos',
              tableName = 'orders',
              columns = ["*"]
            ) 
            SINK("hdfs") OPTIONS (
              filePath = "oss://melin1204/users",
              writeMode = "overwrite"
            )
        """.trimIndent()

        spark.sql(sql)
    }
}