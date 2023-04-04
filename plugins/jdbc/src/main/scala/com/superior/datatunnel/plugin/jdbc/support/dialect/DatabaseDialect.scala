package com.superior.datatunnel.plugin.jdbc.support.dialect

import com.superior.datatunnel.plugin.jdbc.support.JdbcUtils.columnNotFoundInSchemaError
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.conf
import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.types.StructType

import java.sql.{Connection, ResultSet}

abstract class DatabaseDialect {

  def connection: Connection

  protected def getColumns(
      rddSchema: StructType,
      tableSchema: Option[StructType],
      dialect: JdbcDialect): Array[String] = {

    if (tableSchema.isEmpty) {
      rddSchema.fields.map(x => dialect.quoteIdentifier(x.name))
    } else {
      // The generated insert statement needs to follow rddSchema's column sequence and
      // tableSchema's column names. When appending data into some case-sensitive DBMSs like
      // PostgreSQL/Oracle, we need to respect the existing case-sensitive column names instead of
      // RDD column names for user convenience.
      val tableColumnNames = tableSchema.get.fieldNames
      rddSchema.fields.map { col =>
        val normalizedName = tableColumnNames.find(f => conf.resolver(f, col.name)).getOrElse {
          throw columnNotFoundInSchemaError(col, tableSchema)
        }
        dialect.quoteIdentifier(normalizedName)
      }
    }
  }

  def getKeyFieldNames(schema: String, tableName: String): Array[String] = {
    var keyFieldNames = new Array[String](0)
    try {
      val rs: ResultSet = connection.getMetaData.getPrimaryKeys(schema, null, tableName)
      try while (rs.next()) {
        print(rs.getString(1))
        print(rs.getString(2))
        print(rs.getString(3))
        val columnName: String = rs.getString(4)
        keyFieldNames = keyFieldNames :+ columnName
      }
      finally if (rs != null) rs.close()
    }

    keyFieldNames
  }

  def getInsertStatement(
      table: String,
      rddSchema: StructType,
      tableSchema: Option[StructType],
      dialect: JdbcDialect): String = {

    val columns = getColumns(rddSchema, tableSchema, dialect)
    val placeholders = rddSchema.fields.map(_ => "?").mkString(",")
    s"INSERT INTO $table (${columns.mkString(",")}) VALUES ($placeholders)"
  }

  def getUpsertStatement(
      table: String,
      rddSchema: StructType,
      tableSchema: Option[StructType],
      dialect: JdbcDialect): String
}
