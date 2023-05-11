package com.superior.datatunnel.examples.jdbc;

import com.superior.datatunnel.core.DataTunnelExtensions;
import org.apache.spark.sql.SparkSession;

import java.sql.SQLException;

public class DataTunnelPostgres2LogDemo {

    public static void main(String[] args) throws SQLException {
        SparkSession spark = SparkSession
                .builder()
                //.enableHiveSupport()
                .master("local")
                .appName("Datatunnel spark example")
                .config("spark.sql.extensions", DataTunnelExtensions.class.getName())
                .getOrCreate();

        String sql = "datatunnel SOURCE('gauss') OPTIONS(\n" +
                "    username='postgres',\n" +
                "    password='postgres',\n" +
                "    host='172.18.1.56',\n" +
                "    port=5432,\n" +
                "    databaseName='postgres'," +
                "    schemaName='public'," +
                "    tableName='orders', columns=['*'])\n" +
                "    SINK('log') OPTIONS(numRows = 10)";

        spark.sql(sql);
    }
}
