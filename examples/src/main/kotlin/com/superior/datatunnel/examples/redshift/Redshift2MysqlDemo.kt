package com.superior.datatunnel.examples.redshift

import com.superior.datatunnel.core.DataTunnelExtensions
import org.apache.spark.sql.SparkSession

object Redshift2MysqlDemo {

    @JvmStatic
    fun main(args: Array<String>) {
        val accessKeyId = "AKIAW77DWNKCQ6EV6AFI"
        val secretAccessKey = ""
        val iamRole = "arn:aws:iam::480976988805:role/service-role/AmazonRedshift-CommandsAccessRole-20230629T144155"

        val spark = SparkSession
                .builder()
                .master("local")
                .enableHiveSupport()
                .appName("Datatunnel spark example")
                .config("spark.sql.extensions", DataTunnelExtensions::class.java.name)
                .getOrCreate()

        val sql1 = """
            CREATE TEMPORARY VIEW spark_my_table
            USING org.apache.spark.sql.jdbc
            OPTIONS (
              user = "admin",
              password = "Admin2024",
              url = 'jdbc:redshift://redshift-cluster-1.cvytjdhanbq8.us-east-1.redshift.amazonaws.com:5439/dev',
              dbtable "test.my_table",
              tempdir = 's3a://datacyber/redshift_temp/',
              region = 'us-east-1',
              accessKeyId = '${accessKeyId}',
              secretAccessKey = '${secretAccessKey}',
              iamRole = '${iamRole}'
            );
        """.trimIndent()

        spark.sql(sql1)
        //spark.sql("select * from spark_my_table").show()

        val sql = """
            WITH tmp_demo_test2 AS (select id, name userId, age from spark_my_table where age>0)
            datatunnel SOURCE('spark') OPTIONS(
                tableName='tmp_demo_test2')
            SINK("mysql") OPTIONS (
              username = "root",
              password = "root2023",
              host = '172.18.5.44',
              port = 3306,
              schemaName = 'demos',
              tableName = 'users',
              writeMode = 'UPSERT',
              upsertKeyColumns = ['id'],
              columns = ['id', 'userId'])
        """.trimIndent()

        spark.sql(sql)
    }
}