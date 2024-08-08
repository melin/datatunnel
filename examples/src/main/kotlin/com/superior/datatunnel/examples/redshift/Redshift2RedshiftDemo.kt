package com.superior.datatunnel.examples.redshift

import com.superior.datatunnel.core.DataTunnelExtensions
import org.apache.spark.sql.SparkSession

object Redshift2RedshiftDemo {

    @JvmStatic
    fun main(args: Array<String>) {
        val accessKeyId = "xxx"
        val secretAccessKey = "xx"
        val iamRole = "arn:aws:iam::480976988805:role/service-role/AmazonRedshift-CommandsAccessRole-20230629T144155"

        val spark = SparkSession
                .builder()
                .master("local")
                .enableHiveSupport()
                .appName("Datatunnel spark example")
                .config("spark.sql.extensions", DataTunnelExtensions::class.java.name)
                .getOrCreate()

        val sql = """
            DATATUNNEL SOURCE("redshift") OPTIONS (
                username = "admin",
                password = "Admin2024",
                jdbcUrl = 'jdbc:redshift://default-workgroup.480976988805.us-east-1.redshift-serverless.amazonaws.com:5439/dev',
                schemaName = 'public',
                tableName = 'orders',
                tempdir = 's3a://datacyber/redshift_temp/',
                region = 'us-east-1',
                accessKeyId = '${accessKeyId}',
                secretAccessKey = '${secretAccessKey}',
                iamRole = '${iamRole}',
                columns = ["*"]
            )
            SINK("redshift") OPTIONS (
                username = "admin",
                password = "Admin2024",
                jdbcUrl = 'jdbc:redshift://default-workgroup.480976988805.us-east-1.redshift-serverless.amazonaws.com:5439/dev',
                schemaName = 'public',
                tableName = 'orders_sink_1',
                writeMode = 'upsert',
                upsertKeyColumns = ['id'],
                tempdir = 's3a://datacyber/redshift_temp/',
                region = 'us-east-1',
                accessKeyId = '${accessKeyId}',
                secretAccessKey = '${secretAccessKey}',
                preActions = ["CREATE TABLE public.orders_sink_1 AS SELECT * FROM public.orders_sink WHERE 1=2;"],
                iamRole = '${iamRole}',
                columns = ["*"]
            ) 
        """.trimIndent()

        spark.sql(sql)
    }
}