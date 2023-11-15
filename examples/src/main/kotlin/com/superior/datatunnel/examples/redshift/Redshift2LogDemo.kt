package com.superior.datatunnel.examples.redshift

import com.superior.datatunnel.core.DataTunnelExtensions
import org.apache.spark.sql.SparkSession
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sts.StsClient
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest
import software.amazon.awssdk.services.sts.model.GetSessionTokenRequest

object Redshift2LogDemo {

    @JvmStatic
    fun main(args: Array<String>) {
        val accessKeyId = "AKIAW77DWNKCQ6EV6AFI"
        val secretAccessKey = "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"
        val redshiftRoleArn = "arn:aws:iam::480976988805:role/service-role/AmazonRedshift-CommandsAccessRole-20230629T144155"

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
                password = "Admin2023",
                host = 'redshift-cluster-1.cvytjdhanbq8.us-east-1.redshift.amazonaws.com',
                port = 5439,
                databaseName = 'dev',
                schemaName = 'public',
                tableName = 'category',
                tempdir = 's3a://datacyber/redshift_temp/',
                region = 'us-east-1',
                accessKeyId = '${accessKeyId}',
                secretAccessKey = '${secretAccessKey}',
                redshiftRoleArn = '${redshiftRoleArn}',
                columns = ["*"]
            ) 
            SINK("log")
        """.trimIndent()

        spark.sql(sql)
    }
}