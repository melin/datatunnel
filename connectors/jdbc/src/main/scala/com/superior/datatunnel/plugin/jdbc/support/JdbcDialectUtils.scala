package com.superior.datatunnel.plugin.jdbc.support

import com.gitee.melin.bee.util.JdbcUtils
import com.google.common.collect.Lists
import com.superior.datatunnel.api.DataTunnelException
import com.superior.datatunnel.plugin.jdbc.support.dialect._
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.types.{StructField, StructType}

import java.sql.Connection

object JdbcDialectUtils {

  def getDatabaseDialect(conn: Connection, jdbcDialect: JdbcDialect, dataSourceType: String): DefaultDatabaseDialect = {
    if (StringUtils.equalsIgnoreCase("mysql", dataSourceType)) {
      new MySqlDatabaseDialect(conn, jdbcDialect, dataSourceType)
    } else if (StringUtils.equalsIgnoreCase("postgresql", dataSourceType)) {
      new PostgreSqlDatabaseDialect(conn, jdbcDialect, dataSourceType)
    } else if (StringUtils.equalsIgnoreCase("sqlserver", dataSourceType)) {
      new SqlServerMergeDatabaseDialect(conn, jdbcDialect, dataSourceType)
    } else if (StringUtils.equalsIgnoreCase("UNKNOW", dataSourceType)) {
      new OracleMergeDatabaseDialect(conn, jdbcDialect, dataSourceType)
    } else {
      new DefaultDatabaseDialect(conn, jdbcDialect, dataSourceType)
    }
  }

  def invalidJdbcNumPartitionsError(n: Int, jdbcNumPartitions: String): Throwable = {
    new IllegalArgumentException(
      s"Invalid value `$n` for parameter `$jdbcNumPartitions` in table writing " +
        "via JDBC. The minimum value is 1.")
  }

  def queryPrimaryKeys(dataSourceType: String, schemaName: String, tableName: String, conn: Connection): Array[String] = {
    val metaData = conn.getMetaData
    val rs = if ("mysql".equalsIgnoreCase(dataSourceType)) {
      metaData.getPrimaryKeys(schemaName, null, tableName)
    } else {
      metaData.getPrimaryKeys(null, schemaName, tableName)
    }
    val keys: java.util.List[String] = Lists.newArrayList();
    try {
      while (rs.next) {
        val name = rs.getString("COLUMN_NAME")
        keys.add(name)
      }
    } finally {
      JdbcUtils.closeResultSet(rs)
    }

    keys.toArray(new Array[String](0))
  }

  def queryColumns(dataSourceType: String, schemaName: String, tableName: String, conn: Connection): java.util.List[Column] = {
    val metaData = conn.getMetaData
    val rs = if ("mysql".equalsIgnoreCase(dataSourceType)) {
      metaData.getColumns(schemaName, null, tableName, null)
    } else {
      metaData.getColumns(null, schemaName, tableName, null)
    }
    val columns: java.util.List[Column] = Lists.newArrayList();
    try {
      while (rs.next) {
        val column = Column(rs.getString("COLUMN_NAME"), rs.getInt("DATA_TYPE"))
        columns.add(column)
      }
    } finally {
      JdbcUtils.closeResultSet(rs)
    }

    columns
  }

  def columnNotFoundInSchemaError(
      col: StructField, tableSchema: Option[StructType]): Throwable = {
    new DataTunnelException(s"""Column "${col.name}" not found in schema $tableSchema""")
  }
}

case class Column(name: String, jdbcType: Int)
