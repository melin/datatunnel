package com.superior.datatunnel.plugin.jdbc.support.dialect

import com.gitee.melin.bee.core.jdbc.JdbcDialectHolder
import com.gitee.melin.bee.core.jdbc.enums.DataSourceType
import com.gitee.melin.bee.core.jdbc.relational.DatabaseVersion
import com.gitee.melin.bee.util.JdbcUtils
import com.google.common.collect.Lists
import com.superior.datatunnel.plugin.jdbc.support.JdbcDialectUtils._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcOptionsInWrite}
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.{conf, savePartition}
import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.types.StructType

import java.sql.{Connection, Statement}
import java.util
class DefaultDatabaseDialect(connection: Connection, jdbcDialect: JdbcDialect, dataSourceType: String) extends Logging {

  private val dsType = DataSourceType.valueOf(dataSourceType.toUpperCase)
  private val beeJdbcDialect = JdbcDialectHolder.buildJdbcDialect(dsType, connection)

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
    beeJdbcDialect.getSchemas
  }

  def getTableNames(schemaName: String): util.ArrayList[String] = {
    val tableNames = Lists.newArrayList[String]()
    beeJdbcDialect.getSchemaTables(schemaName).forEach(table => tableNames.add(table.getTableName))
    tableNames
  }

  protected def getColumns(
      rddSchema: StructType,
      tableSchema: Option[StructType]): Array[String] = {

    if (tableSchema.isEmpty) {
      rddSchema.fields.map(x => jdbcDialect.quoteIdentifier(x.name))
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
        jdbcDialect.quoteIdentifier(normalizedName)
      }
    }
  }

  def getInsertStatement(
      table: String,
      rddSchema: StructType,
      tableSchema: Option[StructType]): String = {

    val columns = getColumns(rddSchema, tableSchema)
    val placeholders = rddSchema.fields.map(_ => "?").mkString(",")
    s"INSERT INTO $table (${columns.mkString(",")}) VALUES ($placeholders)"
  }

  def getUpsertStatement(
      destTableName: String,
      rddSchema: StructType,
      tableSchema: Option[StructType],
      keyColumns: Array[String]): String = {
    throw new UnsupportedOperationException(s"${dataSourceType} not support operation")
  }

  def saveTable(
       df: DataFrame,
       tableSchema: Option[StructType],
       isCaseSensitive: Boolean,
       options: JdbcOptionsInWrite,
       writeMode: String,
       primaryKeys: Array[String]): Unit = {

    val table = options.table

    val rddSchema = df.schema
    val batchSize = options.batchSize
    val isolationLevel = options.isolationLevel

    val insertStmt = if ("upsert" == writeMode) {
      this.getUpsertStatement(table, rddSchema, tableSchema, primaryKeys)
    } else {
      this.getInsertStatement(table, rddSchema, tableSchema)
    }

    val repartitionedDF = options.numPartitions match {
      case Some(n) if n <= 0 => throw invalidJdbcNumPartitionsError(
        n, JDBCOptions.JDBC_NUM_PARTITIONS)
      case Some(n) if n < df.rdd.getNumPartitions => df.coalesce(n)
      case _ => df
    }
    repartitionedDF.rdd.foreachPartition { iterator =>
      savePartition(
        table, iterator, rddSchema, insertStmt, batchSize, jdbcDialect, isolationLevel, options)
    }
  }

  def bulkInsertTable(
      conn: Connection,
      df: DataFrame,
      options: JdbcOptionsInWrite,
      parameters: Map[String, String],
      primaryKeys: Array[String]): Unit = {
    throw new UnsupportedOperationException(s"${dataSourceType} not support operation")
  }

  protected def executeSql(conn: Connection, sql: String): Unit = {
    var statement: Statement = null
    try {
      statement = conn.createStatement
      statement.execute(sql)
    } finally {
      JdbcUtils.closeStatement(statement)
    }
  }
}
