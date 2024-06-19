package com.superior.datatunnel.plugin.jdbc.support.dialect
import com.gitee.melin.bee.util.JdbcUtils
import com.superior.datatunnel.api.{DataSourceType, DataTunnelException}
import com.superior.datatunnel.plugin.jdbc.support.{
  JdbcDialectUtils,
  LoadDataSqlHelper
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

class MySqlDatabaseDialect(
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

    if (keyColumns.length == 0) {
      throw new IllegalArgumentException("not primary key, not support upsert")
    }

    val conn = jdbcDialect.createConnectionFactory(options)(-1)
    try {
      val version = getDatabaseVersion(conn)
      val columns = getColumns(rddSchema, tableSchema)
      val placeholders = rddSchema.fields.map(_ => "?").mkString(",")

      if (columns.length != rddSchema.fields.length) {
        val msg =
          s"columns 与 rddSchema.fields 数量不一致, ${columns.length}, ${rddSchema.fields.length}"
        throw new DataTunnelException(msg)
      }

      val builder = new StringBuilder()
      var sql =
        s"INSERT INTO $destTableName (${columns.mkString(",")}) VALUES ($placeholders)"
      builder.append(sql)

      if (version.isSameOrAfter(8, 0, 20)) {
        builder.append("\nAS new ON DUPLICATE KEY UPDATE ")
        sql = columns
          .filter(!keyColumns.contains(_))
          .map(col => s"\t$col = new.$col")
          .mkString(",\n")
        builder.append(sql);
      } else {
        builder.append("\nON DUPLICATE KEY UPDATE\n")
        sql = columns
          .filter(!keyColumns.contains(_))
          .map(col => s"\t$col = VALUES($col)")
          .mkString(",\n")
        builder.append(sql);
      }
      builder.toString()
    } finally {
      JdbcUtils.closeConnection(conn)
    }
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
        .map(jdbcDialect.quoteIdentifier)
        .toList
        .asJava
    }

    if (truncate) {
      LogUtils.info(s"prepare truncate table: ${tableId}")
      val sql = s"truncate table ${tableId}";
      executeSql(conn, sql)
    }

    val loadCommand =
      s"LOAD DATA LOCAL INFILE 'datatunnel.csv' REPLACE INTO TABLE ${tableId} " +
        s"FIELDS TERMINATED BY ',' ENCLOSED BY '${'"'}' LINES TERMINATED BY '\n' (${columnNames.asScala
            .mkString(",")})";

    LogUtils.info(s"load data: ${loadCommand}")
    LoadDataSqlHelper.loadData(dataSourceType, parameters)(df, loadCommand)
  }
}
