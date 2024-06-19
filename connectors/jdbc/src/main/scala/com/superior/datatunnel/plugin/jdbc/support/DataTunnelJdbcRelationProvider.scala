package com.superior.datatunnel.plugin.jdbc.support

import com.superior.datatunnel.api.{DataSourceType, DataTunnelException}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils._
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.execution.datasources.jdbc.{
  JdbcOptionsInWrite,
  JdbcRelationProvider,
  JdbcUtils => SparkJdbcUtils
}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.sources.BaseRelation

class DataTunnelJdbcRelationProvider extends JdbcRelationProvider with Logging {

  override def shortName(): String = "datatunnel-jdbc"

  override def createRelation(
      sqlContext: SQLContext,
      saveMode: SaveMode,
      parameters: Map[String, String],
      df: DataFrame
  ): BaseRelation = {

    val options = new JdbcOptionsInWrite(parameters)
    val isCaseSensitive =
      if (sqlContext.getConf(SQLConf.CASE_SENSITIVE.key, "false") == "true")
        true
      else false

    val jdbcDialect = JdbcDialects.get(options.url)
    val conn = jdbcDialect.createConnectionFactory(options)(-1)
    val writeMode = parameters.getOrElse("writeMode", "append")
    val dataSourceType = parameters.getOrElse("dataSourceType", "UNKNOW")
    var upsertKeyColumns =
      StringUtils.split(parameters.getOrElse("upsertKeyColumns", ""), ",")
    upsertKeyColumns = upsertKeyColumns.map(jdbcDialect.quoteIdentifier)

    val databaseDialect = JdbcDialectUtils.getDatabaseDialect(
      options,
      jdbcDialect,
      DataSourceType.valueOf(dataSourceType.toUpperCase)
    )

    try {
      if (StringUtils.endsWithIgnoreCase(writeMode, "BULKINSERT")) {
        if (
          dataSourceType.equalsIgnoreCase("POSTGRESQL") || dataSourceType
            .equalsIgnoreCase("GAUSSDWS")
        ) {
          databaseDialect.bulkInsertTable(
            conn,
            df,
            options,
            parameters,
            upsertKeyColumns
          )
        } else if (
          dataSourceType.equalsIgnoreCase("MYSQL")
          || dataSourceType.equalsIgnoreCase("OCEANBASE")
        ) {
          databaseDialect.bulkInsertTable(
            conn,
            df,
            options,
            parameters,
            upsertKeyColumns
          )
        } else {
          throw new DataTunnelException(
            s"$dataSourceType not support bulk insert"
          )
        }
      } else {
        val tableExists = SparkJdbcUtils.tableExists(conn, options)
        if (tableExists) {
          saveMode match {
            case SaveMode.Overwrite =>
              if (
                options.isTruncate && isCascadingTruncateTable(
                  options.url
                ) == Some(false)
              ) {
                // In this case, we should truncate table and then load.
                truncateTable(conn, options)
                val tableSchema = SparkJdbcUtils.getSchemaOption(conn, options)
                databaseDialect.saveTable(
                  df,
                  tableSchema,
                  isCaseSensitive,
                  options,
                  writeMode,
                  upsertKeyColumns
                )
              } else {
                // Otherwise, do not truncate the table, instead drop and recreate it
                dropTable(conn, options.table, options)
                createTable(
                  conn,
                  options.table,
                  df.schema,
                  isCaseSensitive,
                  options
                )
                databaseDialect.saveTable(
                  df,
                  Some(df.schema),
                  isCaseSensitive,
                  options,
                  writeMode,
                  upsertKeyColumns
                )
              }

            case SaveMode.Append =>
              val tableSchema = SparkJdbcUtils.getSchemaOption(conn, options)
              databaseDialect.saveTable(
                df,
                tableSchema,
                isCaseSensitive,
                options,
                writeMode,
                upsertKeyColumns
              )

            case SaveMode.ErrorIfExists =>
              new DataTunnelException(
                s"Table or view '${options.table}' already exists. SaveMode: ErrorIfExists."
              )

            case SaveMode.Ignore =>
            // With `SaveMode.Ignore` mode, if table already exists, the save operation is expected
            // to not save the contents of the DataFrame and to not change the existing data.
            // Therefore, it is okay to do nothing here and then just return the relation below.
          }
        } else {
          createTable(conn, options.table, df.schema, isCaseSensitive, options)
          databaseDialect.saveTable(
            df,
            Some(df.schema),
            isCaseSensitive,
            options,
            writeMode,
            upsertKeyColumns
          )
        }
      }
    } finally {
      conn.close()
    }

    createRelation(sqlContext, parameters)
  }
}
