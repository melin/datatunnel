package com.superior.datatunnel.examples.jdbc;

import com.superior.datatunnel.core.DataTunnelExtensions;
import org.apache.spark.sql.SparkSession;

public class DataTunnelTidb2LogDemo {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                //.enableHiveSupport()
                .master("local")
                .appName("Datatunnel spark example")
                //  + ",org.apache.spark.sql.TiExtensions"
                .config("spark.sql.extensions", DataTunnelExtensions.class.getName())
                //.config("spark.tispark.pd.addresses", "172.18.5.45:2379")
                //.config("spark.sql.catalog.tidb_catalog", "org.apache.spark.sql.catalyst.catalog.TiCatalog")
                //.config("spark.sql.catalog.tidb_catalog.pd.addresses", "172.18.5.45:2379")
                .getOrCreate();

        String sql = "datatunnel SOURCE('tidb') OPTIONS(\n" +
                "    username='root',\n" +
                "    password='',\n" +
                "    host='172.18.5.45',\n" +
                "    port=4000,\n" +
                "    databaseName='test'," +
                "    tableName='users', columns=['*'])\n" +
                "    SINK('log') OPTIONS(numRows = 10)";

        spark.sql(sql);
    }
}
