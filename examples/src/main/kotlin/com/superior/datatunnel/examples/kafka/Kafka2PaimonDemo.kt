package com.superior.datatunnel.examples.kafka

import com.superior.datatunnel.core.DataTunnelExtensions
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.SparkSession
import java.security.PrivilegedExceptionAction
import org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions

object Kafka2PaimonDemo {

    private val KRB5_FILE = "/Users/melin/Documents/codes/superior/datatunnel/examples/src/main/resources/krb5.conf"
    private val KEYTAB_FILE = "/Users/melin/Documents/codes/superior/datatunnel/examples/src/main/resources/superior.keytab"
    private val PRINCIPAL = "superior/admin@DATACYBER.COM"

    @JvmStatic
    fun main(args: Array<String>) {
        val configuration = Configuration()
        configuration.set("hadoop.security.authentication", "kerberos")
        configuration.set("hadoop.security.authorization", "true")
        // 测试create table
        val createTableSql = """
            CREATE TABLE IF NOT Exists bigdata.paimon_users_kafka (
                id BIGINT, 
                name String, 
                ds string)
            using paimon
            partitioned by (ds) 
            TBLPROPERTIES ('primary-key'='id')
        """.trimIndent()

        loginToKerberos(configuration).doAs(PrivilegedExceptionAction() {
            val spark = SparkSession
                .builder()
                .master("local")
                .enableHiveSupport()
                .appName("Datatunnel spark example")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.sql.catalog.spark_catalog", "org.apache.paimon.spark.SparkGenericCatalog")
                .config("spark.sql.catalog.spark_catalog.type", "hive")
                .config("spark.sql.extensions", DataTunnelExtensions::class.java.name + "," + PaimonSparkSessionExtensions::class.java.name)
                .getOrCreate()

            spark.sql("drop table if exists bigdata.paimon_users_kafka")
            spark.sql(createTableSql)

            val sql = """
            DATATUNNEL SOURCE("kafka") OPTIONS (
                format="json",
                subscribe = "users_json",
                servers = "172.18.5.46:9092",
                includeHeaders = true,
                sourceTempView='tdl_users',
                columns = ['id long', 'name string'],
                checkpointLocation = "/user/superior/stream_checkpoint/datatunnel/paimon_users_kafka"
            ) 
            TRANSFORM = "select id, name, date_format(kafka_timestamp, 'yyyMMdd') as ds from tdl_users"
            SINK("paimon") OPTIONS (
              databaseName = "bigdata",
              tableName = 'paimon_users_kafka',
              columns = ["*"]
            )
        """.trimIndent()

            spark.sql(sql)
        })
    }

    private fun loginToKerberos(configuration: Configuration): UserGroupInformation {
        System.setProperty("java.security.krb5.conf", KRB5_FILE)
        var connectUgi = UserGroupInformation.getCurrentUser()
        if (!connectUgi.isFromKeytab) {
            UserGroupInformation.setConfiguration(configuration)
            // 兼容华为 mrs hive，hw 需要有一个username，UserGroupInformation 登录使用 username
            connectUgi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(PRINCIPAL, KEYTAB_FILE)
        }
        connectUgi.checkTGTAndReloginFromKeytab()
        return connectUgi
    }
}