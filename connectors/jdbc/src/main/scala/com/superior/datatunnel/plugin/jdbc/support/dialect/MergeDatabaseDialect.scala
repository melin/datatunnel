package com.superior.datatunnel.plugin.jdbc.support.dialect

import com.superior.datatunnel.api.{DataSourceType, DataTunnelException}
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.types.StructType

class MergeDatabaseDialect(options: JDBCOptions, jdbcDialect: JdbcDialect, dataSourceType: DataSourceType)
  extends DefaultDatabaseDialect(options, jdbcDialect, dataSourceType) {

  override def getUpsertStatement(
      destTableName: String,
      rddSchema: StructType,
      tableSchema: Option[StructType],
      keyColumns: Array[String]): String = {

    if (keyColumns == null || keyColumns.length == 0) {
      throw new DataTunnelException(s"Cannot write to table $destTableName with no key fields defined.")
    }

    val columns = getColumns(rddSchema, tableSchema)
    val builder = new StringBuffer()
    var sql = s"MERGE INTO $destTableName dist \nUSING (\n    SELECT "
    builder.append(sql);
    sql = columns.map(col => s"? AS $col").mkString(",")
    builder.append(sql);

    if (dataSourceType == DataSourceType.ORACLE) {
      builder.append("\n    FROM DUAL")
    } else if (dataSourceType == DataSourceType.DB2) {
      builder.append("\n    FROM sysibm.sysdummy1")
    }

    builder.append("\n) src\n")
    builder.append("on (")
    sql = keyColumns.map(key => s"src.${key} = dist.${key}").mkString(" AND ")
    builder.append(sql);
    builder.append(")\n")

    val updateColumns = columns.filter(name => !keyColumns.contains(name)).map(name => name)
    if (updateColumns.length == 0) {
      builder.append("WHEN MATCHED THEN\n    DO NOTHING ")
    } else {
      builder.append("WHEN MATCHED THEN\n    UPDATE SET ")
      sql = columns.filter(!keyColumns.contains(_))
        .map(key => s"dist.${key} = src.${key}").mkString(", ")
      builder.append(sql);
    }
    builder.append("\nWHEN NOT MATCHED THEN\n")
    builder.append(s"    insert(${columns.mkString(",")})\n")
    sql = columns.map(col => s"src.${col}").mkString(",")
    builder.append(s"    VALUES ($sql)")
    builder.toString
  }
}
