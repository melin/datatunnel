package com.superior.datatunnel.examples;

import com.superior.datatunnel.core.DataTunnelExtensions;
import org.apache.spark.sql.SparkSession;

public class DataTunnelMutilMysql2LogDemo {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                //.enableHiveSupport()
                .master("local")
                .appName("Datatunnel spark example")
                .config("spark.sql.extensions", DataTunnelExtensions.class.getName())
                .getOrCreate();

        String sql = "datatunnel SOURCE('mysql') OPTIONS(\n" +
                "    username='root',\n" +
                "    password='root2023',\n" +
                "    host='172.18.5.44',\n" +
                "    port=3306,\n" +
                "    schemaName='cdc_demos_[0-9]+', tableName='account_[0-9]+', columns=['*'])\n" +
                "    SINK('log') OPTIONS(numRows = 10)";

        spark.sql(sql);
    }
}
