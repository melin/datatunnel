package com.superior.datatunnel.examples.redshift

import com.superior.datatunnel.core.DataTunnelExtensions
import org.apache.spark.sql.SparkSession

object Mysql2RedshiftDemo {

    @JvmStatic
    fun main(args: Array<String>) {
        val accessKeyId = "xxx"
        val secretAccessKey = "xxx"
        val iamRole = "arn:aws:iam::480976988805:role/service-role/AmazonRedshift-CommandsAccessRole-20230629T144155"

        val spark = SparkSession
                .builder()
                .master("local")
                .enableHiveSupport()
                .appName("Datatunnel spark example")
                .config("spark.sql.extensions", DataTunnelExtensions::class.java.name)
                .getOrCreate()

        val sql = """
            DATATUNNEL SOURCE("mysql") OPTIONS (
                "username" = "admin",
                "password" = "xxxxx",
                "host" = "database-1.c8vfm34zprv8.us-east-1.rds.amazonaws.com",
                port = 3306,
                databaseName = 'demos',
                tableName = 'users',
                columns = ["*"],
                sourceTempView='tdl_users'
            ) 
            TRANSFORM = 'select id, userid, create_time, age from tdl_users'
            SINK("redshift") OPTIONS (
                username = "admin",
                password = "Admin2024",
                jdbcUrl = 'jdbc:redshift://redshift-cluster-1.cvytjdhanbq8.us-east-1.redshift.amazonaws.com:5439/dev',
                schemaName = 'public',
                tableName = 'users',
                writeMode = 'UPSERT',
                upsertKeyColumns = ['id'],
                tempdir = 's3a://datacyber/redshift_temp/',
                region = 'us-east-1',
                accessKeyId = '${accessKeyId}',
                secretAccessKey = '${secretAccessKey}',
                preActions = ["DELETE FROM public.users where id=3"],
                iamRole = '${iamRole}',
                columns = ["*"]
            ) 
        """.trimIndent()

        spark.sql(sql)
    }
}