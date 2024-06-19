package com.superior.datatunnel.plugin.jdbc.support

import com.gitee.melin.bee.util.JdbcUtils
import com.google.common.collect.Lists
import com.superior.datatunnel.api.{DataSourceType, DataTunnelException}
import com.superior.datatunnel.plugin.jdbc.support.dialect._
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.types.{StructField, StructType}

import java.sql.Connection

object JdbcDialectUtils {

  def getDatabaseDialect(
      options: JDBCOptions,
      jdbcDialect: JdbcDialect,
      dataSourceType: DataSourceType
  ): DefaultDatabaseDialect = {
    if (dataSourceType == DataSourceType.MYSQL) {
      new MySqlDatabaseDialect(options, jdbcDialect, dataSourceType)
    } else if (dataSourceType == DataSourceType.POSTGRESQL) {
      new PostgreSqlDatabaseDialect(options, jdbcDialect, dataSourceType)
    } else {
      new MergeDatabaseDialect(options, jdbcDialect, dataSourceType)
    }
  }

  def invalidJdbcNumPartitionsError(
      n: Int,
      jdbcNumPartitions: String
  ): Throwable = {
    new IllegalArgumentException(
      s"Invalid value `$n` for parameter `$jdbcNumPartitions` in table writing " +
        "via JDBC. The minimum value is 1."
    )
  }

  def queryPrimaryKeys(
      dataSourceType: DataSourceType,
      schemaName: String,
      tableName: String,
      conn: Connection
  ): Array[String] = {
    val metaData = conn.getMetaData
    val rs =
      if (
        dataSourceType == DataSourceType.MYSQL ||
        dataSourceType == DataSourceType.ORACLE
      ) {
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

  def queryColumns(
      dataSourceType: DataSourceType,
      schemaName: String,
      tableName: String,
      conn: Connection
  ): java.util.List[Column] = {

    val metaData = conn.getMetaData
    val rs = if (dataSourceType == DataSourceType.MYSQL) {
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
      col: StructField,
      tableSchema: Option[StructType]
  ): Throwable = {
    new DataTunnelException(
      s"""Column "${col.name}" not found in schema $tableSchema"""
    )
  }
}

case class Column(name: String, jdbcType: Int)
