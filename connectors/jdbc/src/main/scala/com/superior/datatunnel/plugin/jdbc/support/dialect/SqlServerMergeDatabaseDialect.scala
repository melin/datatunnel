package com.superior.datatunnel.plugin.jdbc.support.dialect

import org.apache.spark.sql.jdbc.JdbcDialect

import java.sql.Connection

class SqlServerMergeDatabaseDialect(conn: Connection, jdbcDialect: JdbcDialect, dataSourceType: String)
  extends MergeDatabaseDialect(conn, jdbcDialect, dataSourceType) {

}
