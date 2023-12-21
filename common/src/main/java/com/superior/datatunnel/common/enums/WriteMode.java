package com.superior.datatunnel.common.enums;

public enum WriteMode {
    OVERWRITE,
    APPEND,
    UPSERT,
    COPYFROM, //postgres 或者兼容的数据库，https://www.postgresql.org/docs/current/sql-copy.html
    ERROR_IF_EXISTS,
    IGNORE;
}
