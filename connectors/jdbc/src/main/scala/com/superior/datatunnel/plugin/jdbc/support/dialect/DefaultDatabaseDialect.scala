package com.superior.datatunnel.plugin.jdbc.support.dialect

import com.gitee.melin.bee.core.jdbc.relational.DatabaseVersion
import com.gitee.melin.bee.util.JdbcUtils
import com.superior.datatunnel.api.DataSourceType
import com.superior.datatunnel.plugin.jdbc.support.JdbcDialectUtils._
import org.apache.commons.lang3.StringUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcOptionsInWrite}
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.{conf, savePartition}
import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.types.StructType

import java.sql.{Connection, Statement}

class DefaultDatabaseDialect(
    options: JDBCOptions,
    jdbcDialect: JdbcDialect,
    dataSourceType: DataSourceType
) extends Serializable
    with Logging {

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

  protected def getColumns(
      rddSchema: StructType,
      tableSchema: Option[StructType]
  ): Array[String] = {

    val cols = options.parameters.get("columns").get
    if (!"*".contains(cols)) {
      return StringUtils.split(cols, ",").map { col =>
        jdbcDialect.quoteIdentifier(col)
      }
    }

    if (tableSchema.isEmpty) {
      rddSchema.fields.map(x => jdbcDialect.quoteIdentifier(x.name))
    } else {
      // The generated insert statement needs to follow rddSchema's column sequence and
      // tableSchema's column names. When appending data into some case-sensitive DBMSs like
      // PostgreSQL/Oracle, we need to respect the existing case-sensitive column names instead of
      // RDD column names for user convenience.
      val tableColumnNames = tableSchema.get.fieldNames
      rddSchema.fields.map { col =>
        val normalizedName =
          tableColumnNames.find(f => conf.resolver(f, col.name)).getOrElse {
            throw columnNotFoundInSchemaError(col, tableSchema)
          }
        jdbcDialect.quoteIdentifier(normalizedName)
      }
    }
  }

  def getInsertStatement(
      table: String,
      rddSchema: StructType,
      tableSchema: Option[StructType]
  ): String = {

    val columns = getColumns(rddSchema, tableSchema)
    val placeholders = rddSchema.fields.map(_ => "?").mkString(",")
    s"INSERT INTO $table (${columns.mkString(",")}) VALUES ($placeholders)"
  }

  def getUpsertStatement(
      destTableName: String,
      rddSchema: StructType,
      tableSchema: Option[StructType],
      keyColumns: Array[String]
  ): String = {
    throw new UnsupportedOperationException(
      s"${dataSourceType} not support operation"
    )
  }

  def saveTable(
      df: DataFrame,
      tableSchema: Option[StructType],
      isCaseSensitive: Boolean,
      options: JdbcOptionsInWrite,
      writeMode: String,
      primaryKeys: Array[String]
  ): Unit = {

    if ("upsert" == writeMode) {
      this.upsertTable(df, tableSchema, options, primaryKeys)
    } else {
      this.insertTable(df, tableSchema, options)
    }
  }

  private def insertTable(
      df: DataFrame,
      tableSchema: Option[StructType],
      options: JdbcOptionsInWrite
  ): Unit = {
    val table = options.table
    val rddSchema = df.schema
    val insertStmt = this.getInsertStatement(table, rddSchema, tableSchema)
    logInfo(s"insert sql: $insertStmt")
    val batchSize = options.batchSize
    val isolationLevel = options.isolationLevel

    val repartitionedDF = options.numPartitions match {
      case Some(n) if n <= 0 =>
        throw invalidJdbcNumPartitionsError(n, JDBCOptions.JDBC_NUM_PARTITIONS)
      case Some(n) if n < df.rdd.getNumPartitions => df.coalesce(n)
      case _                                      => df
    }
    repartitionedDF.rdd.foreachPartition { iterator =>
      savePartition(
        table,
        iterator,
        rddSchema,
        insertStmt,
        batchSize,
        jdbcDialect,
        isolationLevel,
        options
      )
    }
  }

  private def upsertTable(
      df: DataFrame,
      tableSchema: Option[StructType],
      options: JdbcOptionsInWrite,
      primaryKeys: Array[String]
  ): Unit = {
    val table = options.table
    val rddSchema = df.schema
    val upsertStmt =
      this.getUpsertStatement(table, rddSchema, tableSchema, primaryKeys)
    logInfo(s"upsert sql: $upsertStmt")
    val batchSize = options.batchSize
    val isolationLevel = options.isolationLevel

    val repartitionedDF = options.numPartitions match {
      case Some(n) if n <= 0 =>
        throw invalidJdbcNumPartitionsError(n, JDBCOptions.JDBC_NUM_PARTITIONS)
      case Some(n) if n < df.rdd.getNumPartitions => df.coalesce(n)
      case _                                      => df
    }
    repartitionedDF.rdd.foreachPartition { iterator =>
      savePartition(
        table,
        iterator,
        rddSchema,
        upsertStmt,
        batchSize,
        jdbcDialect,
        isolationLevel,
        options
      )
    }
  }

  def bulkInsertTable(
      conn: Connection,
      df: DataFrame,
      options: JdbcOptionsInWrite,
      parameters: Map[String, String],
      primaryKeys: Array[String]
  ): Unit = {
    throw new UnsupportedOperationException(
      s"${dataSourceType} not support operation"
    )
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
