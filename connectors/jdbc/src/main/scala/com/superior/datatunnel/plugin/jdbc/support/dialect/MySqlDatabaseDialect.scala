package com.superior.datatunnel.plugin.jdbc.support.dialect
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.types.StructType

import java.sql.Connection

class MySqlDatabaseDialect(connection: Connection, jdbcDialect: JdbcDialect, dataSourceType: String)
  extends DatabaseDialect(connection, jdbcDialect, dataSourceType) {

  override def getUpsertStatement(
      table: String,
      rddSchema: StructType,
      tableSchema: Option[StructType]): String = {

    val version = getDatabaseVersion(connection)

    val columns = getColumns(rddSchema, tableSchema)
    val placeholders = rddSchema.fields.map(_ => "?").mkString(",")

    val builder = new StringBuilder()
    var sql = s"INSERT INTO $table (${columns.mkString(",")}) VALUES ($placeholders)"
    builder.append(sql)

    val items = StringUtils.split(table, ".")
    val primaryKeys = this.getKeyFieldNames(items(0), items(1)).map(jdbcDialect.quoteIdentifier)

    if (primaryKeys.length == 0) {
      throw new IllegalArgumentException("not primary key, not support upsert")
    }

    if (version.isSameOrAfter(8, 0, 20)) {
      builder.append("\nAS new ON DUPLICATE KEY UPDATE ")
      sql = columns.filter(!primaryKeys.contains(_))
        .map(col => s"\t$col = new.$col").mkString(",\n")
      builder.append(sql);
    } else {
      builder.append(sql).append("\nON DUPLICATE KEY UPDATE\n")
      sql = columns.filter(!primaryKeys.contains(_))
        .map(col => s"\t$col = VALUES($col)").mkString(",\n")
      builder.append(sql);
    }
    builder.toString()
  }
}
