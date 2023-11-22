package com.superior.datatunnel.examples.maxcompute

import com.superior.datatunnel.core.DataTunnelExtensions
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.SparkSession
import java.security.PrivilegedExceptionAction

object Maxcompute2HiveDemo {

    private val KRB5_FILE = "/Users/melin/Documents/codes/superior/datatunnel/examples/src/main/resources/krb5.conf"
    private val KEYTAB_FILE = "/Users/melin/Documents/codes/superior/datatunnel/examples/src/main/resources/superior.keytab"
    private val PRINCIPAL = "superior/admin@DATACYBER.COM"

    @JvmStatic
    fun main(args: Array<String>) {
        val configuration = Configuration()
        configuration.set("hadoop.security.authentication", "kerberos")
        configuration.set("hadoop.security.authorization", "true")

        System.setProperty("session.catalog.name", "hive_metastore")

        loginToKerberos(configuration).doAs(PrivilegedExceptionAction() {
            val spark = SparkSession
                    .builder()
                    .master("local")
                    .enableHiveSupport()
                    .appName("Datatunnel spark example")
                    .config("spark.sql.extensions", DataTunnelExtensions::class.java.name)
                    .getOrCreate()

            val sql = """
            DATATUNNEL SOURCE("maxcompute") OPTIONS (
              accessKeyId = '',
	          secretAccessKey = '',
	          endpoint = 'http://service.us-east-1.maxcompute.aliyun.com/api',
              projectName = "superior",
              tableName = "orders",
              columns = ["*"]
            )
            SINK("hive") OPTIONS (
              databaseName = "bigdata",
              tableName = 'odps_orders_pt',
              writeMode = 'overwrite',
              partitionColumn = 'pt=20231102',
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