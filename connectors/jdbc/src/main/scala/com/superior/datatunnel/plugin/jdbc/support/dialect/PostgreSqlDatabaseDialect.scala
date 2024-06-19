package com.superior.datatunnel.plugin.jdbc.support.dialect

import com.superior.datatunnel.api.{DataSourceType, DataTunnelException}
import com.superior.datatunnel.plugin.jdbc.support.PostgreSqlHelper.buildUpsertPGSql
import com.superior.datatunnel.plugin.jdbc.support.{
  JdbcDialectUtils,
  PostgreSqlHelper
}
import io.github.melin.jobserver.spark.api.LogUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.jdbc.{
  JDBCOptions,
  JdbcOptionsInWrite
}
import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.types.StructType

import java.sql.Connection
import scala.collection.JavaConverters._

class PostgreSqlDatabaseDialect(
    options: JDBCOptions,
    jdbcDialect: JdbcDialect,
    dataSourceType: DataSourceType
) extends DefaultDatabaseDialect(options, jdbcDialect, dataSourceType) {

  override def getUpsertStatement(
      destTableName: String,
      rddSchema: StructType,
      tableSchema: Option[StructType],
      keyColumns: Array[String]
  ): String = {

    if (keyColumns == null || keyColumns.length == 0) {
      throw new DataTunnelException(
        s"Cannot write to table $destTableName with no key fields defined."
      )
    }

    val columns = getColumns(rddSchema, tableSchema)
    val placeholders = rddSchema.fields.map(_ => "?").mkString(",")

    val updateColumns =
      columns.filter(name => !keyColumns.contains(name)).map(name => name)
    val excludedColumns = updateColumns.map(name => "excluded." + name)

    val sqlBuilder = new StringBuilder()
    val sql =
      s"INSERT INTO $destTableName (${columns.mkString(",")}) VALUES ($placeholders)"
    sqlBuilder
      .append(sql)
      .append("\nON CONFLICT (" + keyColumns.mkString(",") + ") \n")
    if (updateColumns.length == 0) {
      sqlBuilder.append("DO NOTHING \n")
    } else {
      sqlBuilder
        .append("DO UPDATE SET (")
        .append(updateColumns.mkString(","))
        .append(") = ")
        .append("\n")
      sqlBuilder.append("(").append(excludedColumns.mkString(",")).append(")")
    }

    logInfo("upsert sql: " + sqlBuilder.toString())
    sqlBuilder.toString()
  }

  override def bulkInsertTable(
      conn: Connection,
      df: DataFrame,
      options: JdbcOptionsInWrite,
      parameters: Map[String, String],
      primaryKeys: Array[String]
  ): Unit = {
    val truncate = parameters("truncate").toBoolean
    val columnsStr = parameters("columns")
    val schemaName = parameters("schemaName")
    val tableName = parameters("tableName")
    val tableId = options.table;

    val columnNames: java.util.List[String] = if ("*".equals(columnsStr)) {
      JdbcDialectUtils
        .queryColumns(dataSourceType, schemaName, tableName, conn)
        .asScala
        .map(col => {
          jdbcDialect.quoteIdentifier(col.name)
        })
        .toList
        .asJava
    } else {
      StringUtils
        .split(columnsStr, ",")
        .toList
        .map(colName => jdbcDialect.quoteIdentifier(colName))
        .asJava
    }

    LogUtils.info(
      s"table ${tableId} primary keys : ${primaryKeys.mkString(",")}"
    )

    // 创建临时表名
    val items = StringUtils.split(tableId, ".")
    var name = items(items.length - 1)
    name = "datatunnel_temp_" + name + "_001"
    items(items.length - 1) = name
    val tempTableName = items.mkString(".")
    val tempTableMode =
      primaryKeys.length > 0 && !truncate // 设置主键，且truncate = false，才需要创建临时表

    if (truncate) {
      LogUtils.info(s"prepare truncate table: ${tableId}")
      val sql = s"truncate table ${tableId}";
      executeSql(conn, sql)
    }

    if (tempTableMode) {
      LogUtils.info(s"prepare temp table: ${tempTableName}")
      var sql =
        s"CREATE TABLE if not exists ${tempTableName} (LIKE ${tableId} EXCLUDING CONSTRAINTS)";
      executeSql(conn, sql)

      LogUtils.info(s"truncat temp table: ${tempTableName}")
      sql = s"TRUNCATE TABLE ${tempTableName}";
      executeSql(conn, sql)
    }

    if (tempTableMode) {
      // 先导入临时表
      PostgreSqlHelper.copyIn(parameters)(df, tempTableName)
    } else {
      PostgreSqlHelper.copyIn(parameters)(df, tableId)
    }

    if (tempTableMode) {
      // 从临时表导入
      var sql =
        buildUpsertPGSql(tableId, tempTableName, columnNames, primaryKeys)
      LogUtils.info(
        s"import data from ${tempTableName} to ${tableId}, sql: \n${sql}"
      );
      executeSql(conn, sql)

      LogUtils.info(s"drop temp table ${tempTableName}")
      sql = s"drop table $tempTableName";
      executeSql(conn, sql)
    }
  }
}
