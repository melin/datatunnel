package com.superior.datatunnel.examples.postgre

import com.superior.datatunnel.core.DataTunnelExtensions
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.SparkSession
import java.security.PrivilegedExceptionAction

object Hive2PgDemo {

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
                    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

                    //.config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension," + DataTunnelExtensions::class.java.name)
                    //.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
                    //.config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
                    .getOrCreate()

            val sql = """
            WITH temp AS (
                select * from bigdata.ods_jdbc_orders_002
            )
            DATATUNNEL SOURCE("hive") OPTIONS (
              columns = ['*']
            )
            SINK("postgresql") OPTIONS (
              username = "postgres",
              password = "postgres2023",
              host = '172.18.1.56',
              port = 5432,
              databaseName = 'postgres',
              schemaName = 'public',
              tableName = 'orders_002',
              writeMode = 'bulkinsert',
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