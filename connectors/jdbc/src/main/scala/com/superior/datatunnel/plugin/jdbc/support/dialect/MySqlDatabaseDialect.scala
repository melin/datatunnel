package com.superior.datatunnel.plugin.jdbc.support.dialect
import com.google.common.collect.Lists
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.types.StructType

import java.sql.Connection
import java.util

class MySqlDatabaseDialect(connection: Connection, dataSourceType: String)
  extends DatabaseDialect(connection, dataSourceType) {

  override def getSchemaNames: util.ArrayList[String] = {
    val resultSet = connection.getMetaData.getCatalogs
    try {
      val schemaNames = Lists.newArrayList[String]()
      while (resultSet.next) {
        val schemaName = resultSet.getString("TABLE_CAT")
        // skip internal schemas
        if (filterSchema(schemaName)) {
          schemaNames.add(schemaName)
        }
      }
      schemaNames
    } finally {
      if (resultSet != null) resultSet.close()
    }
  }

  override def getTableNames(schemaName: String): util.ArrayList[String] = {
    val metadata = connection.getMetaData
    val resultSet = metadata.getTables(schemaName, null, null, Array("TABLE"))
    try {
      val tableNames = Lists.newArrayList[String]()
      while (resultSet.next) {
        val tableName = resultSet.getString("TABLE_NAME")
        tableNames.add(tableName)
      }

      tableNames
    } finally {
      if (resultSet != null) resultSet.close()
    }
  }

  override def getUpsertStatement(
      table: String,
      rddSchema: StructType,
      tableSchema: Option[StructType],
      dialect: JdbcDialect): String = {

    val columns = getColumns(rddSchema, tableSchema, dialect)
    val placeholders = rddSchema.fields.map(_ => "?").mkString(",")

    val builder = new StringBuilder()
    var sql = s"INSERT INTO $table (${columns.mkString(",")}) VALUES ($placeholders)"
    builder.append(sql).append("\nON DUPLICATE KEY UPDATE\n")

    val items = StringUtils.split(table, ".")
    val primaryKeys = this.getKeyFieldNames(items(0), items(1)).map(dialect.quoteIdentifier)

    if (primaryKeys.length == 0) {
      throw new IllegalArgumentException("not primary key, not support upsert")
    }

    sql = columns.filter(!primaryKeys.contains(_))
      .map(col => s"\t$col = VALUES($col)").mkString(",\n")
    builder.append(sql);
    builder.toString()
  }
}
