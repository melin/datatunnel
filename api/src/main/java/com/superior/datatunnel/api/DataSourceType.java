package com.superior.datatunnel.api;

public enum DataSourceType {
    MYSQL,
    TIDB,
    TISPARK,
    ORACLE,
    SQLSERVER,
    POSTGRESQL,
    TERADATA,
    GAUSS,
    GREENPLUM,
    DB2,
    HANA,
    DAMENG,
    OCEANBASE,

    HIVE,
    KAFKA,
    CLICKHOUSE,
    CASSANDRA,
    HBASE,
    LOG,
    EXCEL,
    SFTP,
    HDFS,
    ELASTICSEARCH;

    public static boolean isJdbcDataSource(DataSourceType dsType) {
        if (dsType == MYSQL || dsType == ORACLE || dsType == SQLSERVER
                || dsType == POSTGRESQL || dsType == DB2 || dsType == TIDB
                || dsType == GAUSS || dsType == GREENPLUM  || dsType == HANA
                || dsType == DAMENG) {
            return true;
        } else {
            return false;
        }
    }
}
