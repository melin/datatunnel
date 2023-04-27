package com.superior.datatunnel.examples.jdbc;

import com.superior.datatunnel.core.DataTunnelExtensions;
import org.apache.spark.sql.SparkSession;

public class DataTunnelMysql2TidbDemo {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                //.enableHiveSupport()
                .master("local")
                .appName("Datatunnel spark example")
                //  + ",org.apache.spark.sql.TiExtensions"
                .config("spark.sql.extensions", DataTunnelExtensions.class.getName() + ",org.apache.spark.sql.TiExtensions")
                .config("spark.tispark.pd.addresses", "172.18.5.45:2379")
                .config("spark.tispark.load_tables", false)
                .config("spark.sql.catalog.tidb_catalog", "org.apache.spark.sql.catalyst.catalog.TiCatalog")
                .config("spark.sql.catalog.tidb_catalog.pd.addresses", "172.18.5.45:2379")
                .getOrCreate();

        String sql = "datatunnel SOURCE('mysql') OPTIONS(\n" +
                "    username='root',\n" +
                "    password='root2023',\n" +
                "    host='172.18.5.44',\n" +
                "    port=3306,\n" +
                "    databaseName='superior', " +
                "    tableName='meta_job', columns=['*'])\n" +
                "SINK('tidb') OPTIONS(\n" +
                "    username='root',\n" +
                "    password='',\n" +
                "    host='172.18.5.45',\n" +
                "    port=4000,\n" +
                "    isolationLevel='NONE'," +
                "    writeMode='upsert'," +
                "    databaseName='test'," +
                "    tableName='meta_job')\n";

        spark.sql(sql);
    }
}
