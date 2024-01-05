package com.superior.datatunnel.plugin.jdbc.support.dialect

import com.superior.datatunnel.api.DataSourceType
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.jdbc.JdbcDialect

class SqlServerMergeDatabaseDialect(options: JDBCOptions, jdbcDialect: JdbcDialect, dataSourceType: DataSourceType)
  extends MergeDatabaseDialect(options, jdbcDialect, dataSourceType) {

}
