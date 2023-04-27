package com.superior.datatunnel.examples.jdbc;

import com.superior.datatunnel.core.DataTunnelExtensions;
import org.apache.spark.sql.SparkSession;

public class DataTunnelMysql2OracleDemo {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                //.enableHiveSupport()
                .master("local")
                .appName("Datatunnel spark example")
                .config("spark.sql.extensions", DataTunnelExtensions.class.getName())
                .getOrCreate();

        String sql = "datatunnel SOURCE('mysql') OPTIONS(\n" +
                "    host='172.18.1.55',\n" +
                "    port=3306,\n" +
                "    username='root',\n" +
                "    password='Datac@123',\n" +
                "    databaseName='datatunnel_demo'," +
                "    tableName='orders'," +
                "    columns=['ORDER_ID', 'CUSTOMER_NAME', 'PRICE', 'PRODUCT_ID', 'ORDER_STATUS'])\n" +
                "SINK('oracle') OPTIONS(\n" +
                "    username='flinkuser',\n" +
                "    password='flinkpw',\n" +
                "    host='172.18.1.56',\n" +
                "    port=1521,\n" +
                "    serviceName='XE',\n" +
                "    databaseName='FLINKUSER'," +
                "    tableName='ORDERS', " +
                "    writeMode='upsert'" +
                ")";
        spark.sql(sql);
    }
}
