package com.superior.datatunnel.examples.jdbc

import com.superior.datatunnel.core.DataTunnelExtensions
import org.apache.spark.sql.SparkSession

object DataTunnelOracle2LogDemo {

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

        val sql = """
            DATATUNNEL SOURCE('oracle') OPTIONS (
              username = 'system',
              password = 'system',
              jdbcUrl = 'jdbc:oracle:thin:@//172.18.1.51:1523/XE',
              databaseName = 'SYSTEM',
              schemaName = 'FLINKUSER',
              tableName = 'Oracle_paimon_spark_oracle',
              columns = ['ID', 'AGE', 'NAME', 'IS_ACTIVE'],
              sourceTempView = 'preview_view'
            ) 
            SINK("log") 
        """.trimIndent()

        spark.sql(sql)
    }
}