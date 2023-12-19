package com.superior.datatunnel.plugin.jdbc.support.dialect

import com.gitee.melin.bee.core.jdbc.JdbcDialectHolder
import com.gitee.melin.bee.core.jdbc.enums.DataSourceType
import com.gitee.melin.bee.core.jdbc.relational.DatabaseVersion
import com.google.common.collect.Lists
import com.superior.datatunnel.plugin.jdbc.support.JdbcDialectUtils.columnNotFoundInSchemaError
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.conf
import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.types.StructType

import java.sql.{Connection, ResultSet}
import java.util

abstract class DatabaseDialect(connection: Connection, dataSourceType: String) extends Logging {

  private val dsType = DataSourceType.valueOf(dataSourceType.toUpperCase)
  private val jdbcDialect = JdbcDialectHolder.buildJdbcDialect(dsType, connection)

  protected def getDatabaseVersion(connection: Connection): DatabaseVersion = {
    val metaData = connection.getMetaData

    try new DatabaseVersion(metaData.getDatabaseProductVersion)
    catch {
      case e: Throwable =>
        try {
          val major = interpretVersion(metaData.getDatabaseMajorVersion)
          val minor = interpretVersion(metaData.getDatabaseMinorVersion)
          new DatabaseVersion(major, minor, 0)
        } catch {
          case e1: IllegalArgumentException =>
            logError("Can't determine database version. Use default")
            new DatabaseVersion(0, 0, 0)
        }
    }
  }

  private def interpretVersion(result: Int) = {
    if (result < 0) -9999
    else result
  }

  def getSchemaNames: util.List[String] = {
    jdbcDialect.getSchemas
  }

  def getTableNames(schemaName: String): util.ArrayList[String] = {
    val tableNames = Lists.newArrayList[String]()
    jdbcDialect.getSchemaTables(schemaName).forEach(table => tableNames.add(table.getTableName))
    tableNames
  }

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
    val rs: ResultSet = connection.getMetaData.getPrimaryKeys(schema, null, tableName)
    try while (rs.next()) {
      val columnName: String = rs.getString(4)
      keyFieldNames = keyFieldNames :+ columnName
    }
    finally if (rs != null) rs.close()

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
      dialect: JdbcDialect): String = {

    return null;
  }
}
