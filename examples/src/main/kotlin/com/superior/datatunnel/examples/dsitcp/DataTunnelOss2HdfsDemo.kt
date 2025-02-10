package com.superior.datatunnel.examples.dsitcp

import com.superior.datatunnel.core.DataTunnelExtensions
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.SparkSession
import java.security.PrivilegedExceptionAction

object DataTunnelOss2HdfsDemo {

    private val KRB5_FILE = "/Users/melin/Documents/codes/superior/datatunnel/examples/src/main/resources/krb5.conf"
    private val KEYTAB_FILE = "/Users/melin/Documents/codes/superior/datatunnel/examples/src/main/resources/superior.keytab"
    private val PRINCIPAL = "superior/admin@DATACYBER.COM"

    @JvmStatic
    fun main(args: Array<String>) {
        val configuration = Configuration()
        configuration.set("hadoop.security.authentication", "kerberos")
        configuration.set("hadoop.security.authorization", "true")

        System.setProperty("spark.default.catalog.name", "hive_metastore")

        loginToKerberos(configuration).doAs(PrivilegedExceptionAction() {

        val spark = SparkSession
            .builder()
            .master("local")
            .enableHiveSupport()
            .appName("Datatunnel spark example")
            .config("spark.sql.extensions", DataTunnelExtensions::class.java.name)
            .config("spark.hadoop.fs.oss.endpoint", "oss-cn-hangzhou.aliyuncs.com")
            .config("spark.hadoop.fs.oss.accessKeyId", "xxx")
            .config("spark.hadoop.fs.oss.accessKeySecret", "xxx")
            .config("spark.hadoop.fs.oss.attempts.maximum", "3")
            .config("spark.hadoop.fs.oss.connection.timeout", "10000")
            .config("spark.hadoop.fs.oss.connection.establish.timeout", "10000")
            .config("spark.hadoop.fs.oss.impl", "org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem")
            .getOrCreate()

        val sql = """
            DISTCP OPTIONS (
              srcPaths = ['oss://melin1204/users'],
              destPath = "hdfs://cdh1:8020/temp",
              overwrite = true,
              delete = true,
              excludes = [".*/_SUCCESS"]
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
            connectUgi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
                PRINCIPAL,
                KEYTAB_FILE
            )
        }
        connectUgi.checkTGTAndReloginFromKeytab()
        return connectUgi
    }
}