package com.superior.datatunnel.examples.kafka

import com.superior.datatunnel.core.DataTunnelExtensions
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hudi.HoodieSparkSessionExtension

object Kafka2HudiDemo {

    @JvmStatic
    fun main(args: Array<String>) {
        System.setProperty("HADOOP_USER_NAME", "hdfs");

        val spark = SparkSession
            .builder()
            .master("local")
            .enableHiveSupport()
            .appName("Datatunnel spark example")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.sql.extensions", DataTunnelExtensions::class.java.name
                    + "," + HoodieSparkSessionExtension::class.java.name)
            .getOrCreate()

        // date_format(unix_millis(timestamp), 'yyyyMMddHH')
        val sql = """
            DATATUNNEL SOURCE("kafka") OPTIONS (
                subscribe = "orders",
                servers = "3.208.89.140:9092",
                includeHeaders = true,
                checkpointLocation = "/user/dataworks/stream_checkpoint/datatunnel/tdl_users",
                resultTableName='tdl_users'
            )
            TRANSFORM = "select cast(timestamp as string) as id, 
                    cast(value as string) as message, timestamp as kafka_timestamp, 
                    '20230706' ds from tdl_users"
            SINK("hudi") OPTIONS (
              databaseName = "bigdata",
              tableName = 'hudi_orders_mor',
              writeMode = 'overwrite',
              columns = ["*"]
            )
        """.trimIndent()

        spark.sql(sql)

        // 测试create table
        """
            CREATE TABLE bigdata.hudi_orders_mor (
                id String, 
                message String, 
                kafka_timestamp bigint,
                ds string)
            using hudi
            partitioned by (ds) 
            tblproperties (
              type = 'mor',
              primaryKey = 'id',
              preCombineField = 'kafka_timestamp'
             )
            lifeCycle 300
        """.trimIndent()
    }
}