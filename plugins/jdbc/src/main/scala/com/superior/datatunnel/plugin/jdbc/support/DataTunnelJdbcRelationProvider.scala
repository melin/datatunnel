package com.superior.datatunnel.plugin.jdbc.support

import com.superior.datatunnel.api.DataTunnelException
import com.superior.datatunnel.plugin.jdbc.support.JdbcUtils.saveTable
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.{createTable, dropTable, isCascadingTruncateTable, truncateTable}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.execution.datasources.jdbc.{JdbcOptionsInWrite, JdbcRelationProvider, JdbcUtils => SparkJdbcUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.sources.BaseRelation

class DataTunnelJdbcRelationProvider extends JdbcRelationProvider {

  override def shortName(): String = "datatunnel-jdbc"

  override def createRelation(
       sqlContext: SQLContext,
       mode: SaveMode,
       parameters: Map[String, String],
       df: DataFrame): BaseRelation = {

    val options = new JdbcOptionsInWrite(parameters)
    val isCaseSensitive = if (sqlContext.getConf(SQLConf.CASE_SENSITIVE.key, "false") == "true") true else false

    val dialect = JdbcDialects.get(options.url)
    val conn = dialect.createConnectionFactory(options)(-1)
    val writeMode = parameters.getOrElse("writeMode", "insert")
    try {
      val tableExists = SparkJdbcUtils.tableExists(conn, options)
      if (tableExists) {
        mode match {
          case SaveMode.Overwrite =>
            if (options.isTruncate && isCascadingTruncateTable(options.url) == Some(false)) {
              // In this case, we should truncate table and then load.
              truncateTable(conn, options)
              val tableSchema = SparkJdbcUtils.getSchemaOption(conn, options)
              saveTable(conn, df, tableSchema, isCaseSensitive, options, writeMode)
            } else {
              // Otherwise, do not truncate the table, instead drop and recreate it
              dropTable(conn, options.table, options)
              createTable(conn, options.table, df.schema, isCaseSensitive, options)
              saveTable(conn, df, Some(df.schema), isCaseSensitive, options, writeMode)
            }

          case SaveMode.Append =>
            val tableSchema = SparkJdbcUtils.getSchemaOption(conn, options)
            saveTable(conn, df, tableSchema, isCaseSensitive, options, writeMode)

          case SaveMode.ErrorIfExists =>
            new DataTunnelException(
              s"Table or view '${options.table}' already exists. SaveMode: ErrorIfExists.")

          case SaveMode.Ignore =>
          // With `SaveMode.Ignore` mode, if table already exists, the save operation is expected
          // to not save the contents of the DataFrame and to not change the existing data.
          // Therefore, it is okay to do nothing here and then just return the relation below.
        }
      } else {
        createTable(conn, options.table, df.schema, isCaseSensitive, options)
        saveTable(conn, df, Some(df.schema), isCaseSensitive, options, writeMode)
      }
    } finally {
      conn.close()
    }

    createRelation(sqlContext, parameters)
  }
}
