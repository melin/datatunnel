package com.superior.datatunnel.plugin.jdbc.support

import com.superior.datatunnel.api.DataTunnelException
import com.superior.datatunnel.plugin.jdbc.support.JdbcDialectUtils.saveTable
import com.superior.datatunnel.plugin.jdbc.support.PostgreSqlHelper.buildUpsertPGSql
import io.github.melin.jobserver.spark.api.LogUtils
import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.{StringUtils, SystemUtils}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.{createTable, dropTable, isCascadingTruncateTable, truncateTable}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.execution.datasources.jdbc.{JdbcOptionsInWrite, JdbcRelationProvider, JdbcUtils => SparkJdbcUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.sources.BaseRelation

import java.io.File
import java.sql.{Connection, Statement}
import scala.collection.JavaConverters._

class DataTunnelJdbcRelationProvider extends JdbcRelationProvider with Logging {

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
    val writeMode = parameters.getOrElse("writeMode", "append")
    val dataSourceType = parameters.getOrElse("dataSourceType", "UNKNOW")
    try {
      if (StringUtils.endsWithIgnoreCase(writeMode, "BULKINSERT")) {
        if (dataSourceType.equalsIgnoreCase("POSTGRESQL") || dataSourceType.equalsIgnoreCase("GAUSSDWS")) {
          bulkInsertPG(conn, df, options, parameters)
        } else if (dataSourceType.equalsIgnoreCase("MYSQL")) {
          bulkInsertMySql(conn, df, options, parameters)
        } else {
          throw new DataTunnelException(s"$dataSourceType not support bulk insert")
        }
      } else {
        val tableExists = SparkJdbcUtils.tableExists(conn, options)
        if (tableExists) {
          mode match {
            case SaveMode.Overwrite =>
              if (options.isTruncate && isCascadingTruncateTable(options.url) == Some(false)) {
                // In this case, we should truncate table and then load.
                truncateTable(conn, options)
                val tableSchema = SparkJdbcUtils.getSchemaOption(conn, options)
                saveTable(conn, df, tableSchema, isCaseSensitive, options, writeMode, dataSourceType)
              } else {
                // Otherwise, do not truncate the table, instead drop and recreate it
                dropTable(conn, options.table, options)
                createTable(conn, options.table, df.schema, isCaseSensitive, options)
                saveTable(conn, df, Some(df.schema), isCaseSensitive, options, writeMode, dataSourceType)
              }

            case SaveMode.Append =>
              val tableSchema = SparkJdbcUtils.getSchemaOption(conn, options)
              saveTable(conn, df, tableSchema, isCaseSensitive, options, writeMode, dataSourceType)

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
          saveTable(conn, df, Some(df.schema), isCaseSensitive, options, writeMode, dataSourceType)
        }
      }
    } finally {
      conn.close()
    }

    createRelation(sqlContext, parameters)
  }

  // pg bulk insert
  private def bulkInsertPG(
      conn: Connection,
      df: DataFrame,
      options: JdbcOptionsInWrite,
      parameters: Map[String, String]): Unit = {

    val truncate = parameters("truncate").toBoolean
    val columnsStr = parameters("columns")
    val dsType = parameters("dsType")
    val schemaName = parameters("schemaName")
    val tableName = parameters("tableName")
    val tableId = options.table;

    val primaryKeys = JdbcDialectUtils.queryPrimaryKeys(dsType, schemaName, tableName, conn)
    val columnNames: java.util.List[String] = if ("*".equals(columnsStr))
      JdbcDialectUtils.queryColumns(dsType, schemaName, tableName, conn).asScala.map(col => col.name).toList.asJava
    else StringUtils.split(columnsStr, ",").toList.asJava

    logInfo(s"table ${tableId} primary keys : ${primaryKeys.asScala.mkString(",")}")
    LogUtils.info(s"table ${tableId} primary keys : ${primaryKeys.asScala.mkString(",")}")

    // 创建临时表名
    val items = StringUtils.split(tableId, ".")
    var name = items(items.length - 1)
    name = "datatunnel_temp_" + name + "_001"
    items(items.length - 1) = name
    val tempTableName = items.mkString(".")
    val tempTableMode = primaryKeys.size() > 0 && !truncate // 设置主键，且truncate = false，才需要创建临时表

    if (truncate) {
      logInfo(s"prepare truncate table: ${tableId}")
      LogUtils.info(s"prepare truncate table: ${tableId}")
      val sql = s"truncate table ${tableId}";
      executeSql(conn, sql)
    }

    if (tempTableMode) {
      logInfo(s"prepare temp table: ${tempTableName}")
      LogUtils.info(s"prepare temp table: ${tempTableName}")
      var sql = s"CREATE TABLE if not exists ${tempTableName} (LIKE ${tableId} EXCLUDING CONSTRAINTS)";
      executeSql(conn, sql)

      logInfo(s"truncat temp table: ${tempTableName}");
      LogUtils.info(s"truncat temp table: ${tempTableName}")
      sql = s"TRUNCATE TABLE ${tempTableName}";
      executeSql(conn, sql)
    }

    if (tempTableMode) {
      //先导入临时表
      PostgreSqlHelper.copyIn(parameters)(df, tempTableName)
    } else {
      PostgreSqlHelper.copyIn(parameters)(df, tableId)
    }

    if (tempTableMode) {
      // 从临时表导入
      var sql = buildUpsertPGSql(tableId, tempTableName, columnNames, primaryKeys)
      logInfo(s"import data from ${tempTableName} to ${tableId}, sql: \n${sql}");
      LogUtils.info(s"import data from ${tempTableName} to ${tableId}, sql: \n${sql}");
      executeSql(conn, sql)

      logInfo(s"drop temp table ${tempTableName}");
      LogUtils.info(s"drop temp table ${tempTableName}")
      sql = s"drop table $tempTableName";
      executeSql(conn, sql)
    }
  }

  // pg bulk insert
  private def bulkInsertMySql(
      conn: Connection,
      df: DataFrame,
      options: JdbcOptionsInWrite,
      parameters: Map[String, String]): Unit = {

    val truncate = parameters("truncate").toBoolean
    val columnsStr = parameters("columns")
    val dsType = parameters("dsType")
    val schemaName = parameters("schemaName")
    val tableName = parameters("tableName")
    val tableId = options.table;

    val primaryKeys = JdbcDialectUtils.queryPrimaryKeys(dsType, schemaName, tableName, conn)
    val columnNames: java.util.List[String] = if ("*".equals(columnsStr))
      JdbcDialectUtils.queryColumns(dsType, schemaName, tableName, conn).asScala.map(col => col.name).toList.asJava
    else StringUtils.split(columnsStr, ",").toList.asJava

    logInfo(s"table ${tableId} primary keys : ${primaryKeys.asScala.mkString(",")}")
    LogUtils.info(s"table ${tableId} primary keys : ${primaryKeys.asScala.mkString(",")}")

    if (truncate) {
      logInfo(s"prepare truncate table: ${tableId}")
      LogUtils.info(s"prepare truncate table: ${tableId}")
      val sql = s"truncate table ${tableId}";
      executeSql(conn, sql)
    }

    val path = SystemUtils.USER_DIR + "/" + tableName
    logInfo("files: " + path)
    val filePath = MysqlSqlHelper.rowsToFile(df, path)

    val loadCommand = s"LOAD DATA LOCAL INFILE '${filePath}' REPLACE INTO TABLE ${tableId} " +
      s"FIELDS TERMINATED BY ',' ENCLOSED BY '${'"'}' LINES TERMINATED BY '\n' (${columnNames.asScala.mkString(",")})";

    logInfo(s"load data: ${loadCommand}")
    LogUtils.info(s"load data: ${loadCommand}")
    executeSql(conn, loadCommand)
    // 删除文件，避免占用磁盘
    FileUtils.deleteQuietly(new File(filePath))
  }

  private def executeSql(conn: Connection, sql: String): Unit = {
    var statement: Statement = null
    try {
      statement = conn.createStatement
      statement.execute(sql)
    } finally {
      try {
        if (statement != null) {
          statement.close()
        }
      } catch {
        case _: Throwable =>
      }
    }
  }
}
