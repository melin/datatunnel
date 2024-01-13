package com.superior.datatunnel.api;

public enum DataSourceType {
    MYSQL,
    TISPARK,
    ORACLE,
    SQLSERVER,
    POSTGRESQL,
    TERADATA,
    GAUSSDWS,
    GREENPLUM,
    DB2,
    HANA,
    DAMENG,
    OCEANBASE,
    MAXCOMPUTE,
    REDSHIFT,
    SNOWFLAKE,
    DORIS,
    STARROCKS,

    HIVE,
    CLICKHOUSE,
    CASSANDRA,
    HBASE,
    LOG,
    EXCEL,
    SFTP,
    FTP,
    S3,
    HDFS,
    REDIS,
    ELASTICSEARCH;

    public static boolean isJdbcDataSource(DataSourceType dsType) {
        if (dsType == MYSQL || dsType == ORACLE || dsType == SQLSERVER
                || dsType == POSTGRESQL || dsType == DB2
                || dsType == GAUSSDWS || dsType == GREENPLUM  || dsType == HANA
                || dsType == DAMENG) {
            return true;
        } else {
            return false;
        }
    }
}
