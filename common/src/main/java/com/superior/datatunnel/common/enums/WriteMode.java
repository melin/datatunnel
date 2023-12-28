package com.superior.datatunnel.common.enums;

public enum WriteMode {
    OVERWRITE,
    APPEND,
    UPSERT,
    // postgres 或者兼容的数据库，https://www.postgresql.org/docs/current/sql-copy.html
    // mysql LOAD DATA https://github.com/xiaobing007/kettle/blob/master/engine/src/org/pentaho/di/trans/steps/mysqlbulkloader/MySQLBulkLoader.java
    // sqlserver
    BULKINSERT
}
