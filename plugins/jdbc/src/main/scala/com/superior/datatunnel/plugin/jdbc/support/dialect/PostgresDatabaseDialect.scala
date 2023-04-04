package com.superior.datatunnel.plugin.jdbc.support.dialect

import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.types.StructType

import java.sql.Connection

class PostgresDatabaseDialect(conn: Connection) extends DatabaseDialect {

  override def connection: Connection = conn

  def getUpsertStatement(
      table: String,
      rddSchema: StructType,
      tableSchema: Option[StructType],
      dialect: JdbcDialect): String = {

    this.getInsertStatement(table, rddSchema, tableSchema, dialect)
  }
}
