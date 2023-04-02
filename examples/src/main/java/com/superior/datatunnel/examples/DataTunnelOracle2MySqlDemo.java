package com.superior.datatunnel.examples;

import com.superior.datatunnel.core.DataTunnelExtensions;
import org.apache.spark.sql.SparkSession;

public class DataTunnelOracle2MySqlDemo {

    public static void main(String[] args) {
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
                "SINK('mysql') OPTIONS(\n" +
                "    host='172.18.1.55',\n" +
                "    port=3306,\n" +
                "    username='root',\n" +
                "    password='Datac@123',\n" +
                "    databaseName='datatunnel_demo'," +
                "    tableName='orders'," +
                "    writeMode='upsert'" +
                ")";
        spark.sql(sql);
    }
}
