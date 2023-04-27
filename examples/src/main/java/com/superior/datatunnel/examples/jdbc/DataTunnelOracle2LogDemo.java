package com.superior.datatunnel.examples.jdbc;

import com.superior.datatunnel.core.DataTunnelExtensions;
import org.apache.spark.sql.SparkSession;

import java.sql.SQLException;

public class DataTunnelOracle2LogDemo {

    public static void main(String[] args) throws SQLException {
        SparkSession spark = SparkSession
                .builder()
                //.enableHiveSupport()
                .master("local")
                .appName("Datatunnel spark example")
                .config("spark.sql.extensions", DataTunnelExtensions.class.getName())
                .getOrCreate();

        String sql = "datatunnel SOURCE('oracle') OPTIONS(\n" +
                "    username='flinkuser',\n" +
                "    password='flinkpw',\n" +
                "    host='172.18.1.56',\n" +
                "    port=1521,\n" +
                "    serviceName='XE',\n" +
                "    databaseName='FLINKUSER'," +
                "    tableName='ORDERS', columns=['ORDER_ID', 'CUSTOMER_NAME', 'PRICE', 'PRODUCT_ID', 'ORDER_STATUS'])\n" +
                "    SINK('log') OPTIONS(numRows = 10)";

        spark.sql(sql);
    }
}
