package com.superior.datatunnel.plugin.jdbc.support

import com.superior.datatunnel.api.DataTunnelException
import com.superior.datatunnel.plugin.jdbc.support.dialect.MySqlDatabaseDialect
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcOptionsInWrite}
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.savePartition
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.types.{StructField, StructType}

import java.sql.Connection

object JdbcUtils {

  def saveTable(
       conn: Connection,
       df: DataFrame,
       tableSchema: Option[StructType],
       isCaseSensitive: Boolean,
       options: JdbcOptionsInWrite,
       writeMode: String): Unit = {

    val url = options.url
    val table = options.table
    val dialect = JdbcDialects.get(url)
    val rddSchema = df.schema
    val batchSize = options.batchSize
    val isolationLevel = options.isolationLevel

    val databaseDialect = getDatabaseDialect(conn)
    val insertStmt = if ("upsert" == writeMode) {
      databaseDialect.getUpsertStatement(table, rddSchema, tableSchema, dialect)
    } else {
      databaseDialect.getInsertStatement(table, rddSchema, tableSchema, dialect)
    }

    val repartitionedDF = options.numPartitions match {
      case Some(n) if n <= 0 => throw invalidJdbcNumPartitionsError(
        n, JDBCOptions.JDBC_NUM_PARTITIONS)
      case Some(n) if n < df.rdd.getNumPartitions => df.coalesce(n)
      case _ => df
    }
    repartitionedDF.rdd.foreachPartition { iterator =>
      savePartition(
        table, iterator, rddSchema, insertStmt, batchSize, dialect, isolationLevel, options)
    }
  }

  private def getDatabaseDialect(conn: Connection): Unit = {
    val databaseDialect = new MySqlDatabaseDialect(conn)
  }

  private def invalidJdbcNumPartitionsError(n: Int, jdbcNumPartitions: String): Throwable = {
    new IllegalArgumentException(
      s"Invalid value `$n` for parameter `$jdbcNumPartitions` in table writing " +
        "via JDBC. The minimum value is 1.")
  }

  def columnNotFoundInSchemaError(
      col: StructField, tableSchema: Option[StructType]): Throwable = {
    new DataTunnelException(s"""Column "${col.name}" not found in schema $tableSchema""")
  }
}
