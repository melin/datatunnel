package com.superior.datatunnel.api;

public enum DataSourceType {
    MYSQL,
    ORACLE,
    SQLSERVER,
    POSTGRESQL,
    DB2,

    HIVE,
    KAFKA,
    CLICKHOUSE,
    HBASE,
    LOG,
    FILE,
    SFTP,
    HDFS,
    ELASTICSEARCH;

    public static boolean isJdbcDataSource(DataSourceType dsType) {
        if (dsType == MYSQL || dsType == ORACLE || dsType == SQLSERVER || dsType == POSTGRESQL || dsType == DB2) {
            return true;
        } else {
            return false;
        }
    }
}
