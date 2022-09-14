package com.superior.datatunnel.api;

public enum DataSourceType {
    MYSQL,
    TIDB,
    ORACLE,
    SQLSERVER,
    POSTGRESQL,
    TERADATA,
    GAUSS,
    GREENPLUM,
    DB2,
    HANA,

    HIVE,
    KAFKA,
    CLICKHOUSE,
    HBASE,
    LOG,
    EXCEL,
    SFTP,
    HDFS,
    ELASTICSEARCH;

    public static boolean isJdbcDataSource(DataSourceType dsType) {
        if (dsType == MYSQL || dsType == ORACLE || dsType == SQLSERVER
                || dsType == POSTGRESQL || dsType == DB2 || dsType == TIDB
                || dsType == GAUSS || dsType == GREENPLUM  || dsType == HANA) {
            return true;
        } else {
            return false;
        }
    }
}
