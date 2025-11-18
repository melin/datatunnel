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
    HASHDATA,
    DB2,
    HANA,
    DAMENG,
    OCEANBASE,
    MAXCOMPUTE,
    REDSHIFT,
    SNOWFLAKE,
    DORIS,
    STARROCKS,
    KINGBASEES,

    HIVE,
    HUDI,
    PAIMON,
    DELTA,
    ICEBERG,

    SPARK, // HIVE 别名
    KAFKA,
    CLICKHOUSE,
    CASSANDRA,
    HBASE,
    MONGODB,
    LOG,
    EXCEL,
    SFTP,
    FTP,
    S3,
    OSS,
    COS,
    MINIO,
    HDFS,
    REDIS,
    ELASTICSEARCH;

    public static boolean isJdbcDataSource(DataSourceType dsType) {
        if (dsType == MYSQL
                || dsType == ORACLE
                || dsType == SQLSERVER
                || dsType == POSTGRESQL
                || dsType == DB2
                || dsType == GAUSSDWS
                || dsType == GREENPLUM
                || dsType == HANA
                || dsType == DAMENG
                || dsType == KINGBASEES) {
            return true;
        } else {
            return false;
        }
    }
}
