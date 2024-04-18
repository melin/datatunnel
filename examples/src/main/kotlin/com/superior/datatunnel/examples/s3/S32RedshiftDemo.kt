package com.superior.datatunnel.examples.s3

import com.superior.datatunnel.core.DataTunnelExtensions
import org.apache.spark.sql.SparkSession

object S32RedshiftDemo {

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
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSyste")
            .getOrCreate()

        val sql = """
            DATATUNNEL SOURCE("s3") OPTIONS (
                format = "json",
                filePath = "s3a://datacyber/melin1204/",
                region = "us-east-1",
                sourceTempView='tdl_users'
            ) 
            TRANSFORM = 'select id, userid, age from tdl_users'
            SINK("redshift") OPTIONS (
                username = "admin",
                password = "Admin2024",
                jdbcUrl = "jdbc:redshift://redshift-cluster-1.cvytjdhanbq8.us-east-1.redshift.amazonaws.com:5439/dev",
                schemaName = "public",
                tableName = "users1",
                writeMode = "UPSERT",
                upsertKeyColumns = ["id"],
                tempdir = "s3a://datacyber/redshift_temp/",
                region = "us-east-1",
                accessKeyId = "${accessKeyId}",
                secretAccessKey = "${secretAccessKey}",
                iamRole = "${iamRole}",
                columns = ["*"]
            ) 
        """.trimIndent()

        spark.sql(sql)
    }
}