package com.superior.datatunnel.examples.s3

import com.superior.datatunnel.core.DataTunnelExtensions
import org.apache.spark.sql.SparkSession

object Redshift2S3Demo {

    @JvmStatic
    fun main(args: Array<String>) {
        val accessKeyId = "xx"
        val secretAccessKey = "xx"
        val iamRole = "arn:aws:iam::480976988805:role/service-role/AmazonRedshift-CommandsAccessRole-20230629T144155"

        val spark = SparkSession
            .builder()
            .master("local")
            .enableHiveSupport()
            .appName("Datatunnel spark example")
            .config("spark.sql.extensions", DataTunnelExtensions::class.java.name)

            .config("spark.hadoop.fs.s3a.access.key", accessKeyId)
            .config("spark.hadoop.fs.s3a.secret.key", secretAccessKey)
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .getOrCreate()

        val sql = """
            DATATUNNEL SOURCE("redshift") OPTIONS (
                username = "admin",
                password = "Admin2024",
                jdbcUrl = "jdbc:redshift://redshift-cluster-1.cvytjdhanbq8.us-east-1.redshift.amazonaws.com:5439/dev",
                schemaName = "public",
                tableName = "users1",
                tempdir = "s3a://datacyber/redshift_temp/",
                region = "us-east-1",
                accessKeyId = "${accessKeyId}",
                secretAccessKey = "${secretAccessKey}",
                iamRole = "${iamRole}",
                columns = ["*"],
                sourceTempView='tdl_users'
            )
            TRANSFORM = 'select id, userid, age from tdl_users'
            SINK ("s3") OPTIONS (
                format = "csv",
                path = "s3a://datacyber/melin1204/20240419",
                writeMode = "overwrite",
                region = "us-east-1",
                compression = "GZIP"
            ) 
        """.trimIndent()

        spark.sql(sql)
    }
}