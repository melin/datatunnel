package com.superior.datatunnel.plugin.jdbc.support.dialect
import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.types.StructType

import java.sql.Connection

class MySqlDatabaseDialect(conn: Connection) extends DatabaseDialect {

  def getUpsertStatement(
      table: String,
      rddSchema: StructType,
      tableSchema: Option[StructType],
      dialect: JdbcDialect): String = {

    val columns = getColumns(rddSchema, tableSchema, dialect)
    val placeholders = rddSchema.fields.map(_ => "?").mkString(",")

    val builder = new StringBuilder()
    var sql = s"INSERT INTO $table (${columns.mkString(",")}) VALUES ($placeholders)"
    builder.append(sql).append("\nON DUPLICATE KEY UPDATE\n")
    sql = columns.map(col => s"\t$col = VALUES($col)").mkString(",\n")
    builder.append(sql);
    builder.toString()
  }
}
