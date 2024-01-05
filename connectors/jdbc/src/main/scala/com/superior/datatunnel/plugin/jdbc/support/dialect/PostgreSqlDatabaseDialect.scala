package com.superior.datatunnel.plugin.jdbc.support.dialect

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.types.StructType

import java.sql.Connection

class PostgreSqlDatabaseDialect(connection: Connection, jdbcDialect: JdbcDialect, dataSourceType: String)
  extends DatabaseDialect(connection, jdbcDialect, dataSourceType) {

  override def getUpsertStatement(
      table: String,
      rddSchema: StructType,
      tableSchema: Option[StructType]): String = {

    val columns = getColumns(rddSchema, tableSchema)
    val placeholders = rddSchema.fields.map(_ => "?").mkString(",")

    val items = StringUtils.split(table, ".")
    val primaryKeys = this.getKeyFieldNames(items(0), items(1)).map(jdbcDialect.quoteIdentifier)

    val builder = new StringBuilder()
    val sql = s"INSERT INTO $table (${columns.mkString(",")}) VALUES ($placeholders)"
    builder.append(sql).append("\nON CONFLICT (" + primaryKeys.mkString(",") + ") \nDO NOTHING;\n")

    if (primaryKeys.length == 0) {
      throw new IllegalArgumentException("not primary key, not support upsert")
    }
    builder.toString()
  }
}
