package com.superior.datatunnel.examples;

import com.superior.datatunnel.core.DataTunnelExtensions;
import org.apache.spark.sql.SparkSession;

public class DataTunnelCassandra2LogDemo {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("Datatunnel spark example")
                .config("spark.sql.extensions", DataTunnelExtensions.class.getName())
                .getOrCreate();

        String sql = "datatunnel SOURCE('cassandra') OPTIONS(\n" + "    username='cassandra',\n"
                + "    password='cassandra',\n"
                + "    host='172.18.1.56',\n"
                + "    port=19042,\n"
                + "    keyspace='store',"
                + "    tableName='shopping_cart', "
                + "    columns=['*'])\n"
                + "SINK('log') OPTIONS(numRows = 10)";

        spark.sql(sql);
    }
}
