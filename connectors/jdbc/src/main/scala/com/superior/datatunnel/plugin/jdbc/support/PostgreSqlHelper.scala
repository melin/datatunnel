package com.superior.datatunnel.plugin.jdbc.support

import com.gitee.melin.bee.util.JdbcUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.jdbc.JdbcOptionsInWrite
import org.apache.spark.sql.jdbc.JdbcDialects

import java.io.InputStream
import org.apache.spark.sql.{DataFrame, Row}
import org.postgresql.copy.CopyManager
import org.postgresql.core.BaseConnection

import java.nio.ByteBuffer
import java.util
import scala.collection.JavaConverters._

// https://gist.github.com/longcao/bb61f1798ccbbfa4a0d7b76e49982f84
object PostgreSqlHelper extends Logging{

  private val fieldDelimiter = ",";

  def rowsToInputStream(rows: Iterator[Row]): InputStream = {
    val bytes: Iterator[Byte] = rows.flatMap {
      row => {
        val columns = row.toSeq.map { v =>
          if (v == null) {
            Array[Byte]('\\', 'N')
          } else {
            v.toString.getBytes()
          }
        }

        val bytesSize = columns.map(_.length).sum
        val byteBuffer = ByteBuffer.allocate((bytesSize * 2 + 10).toInt)

        var index: Int = 0;
        columns.foreach(bytes => {
          if (index > 0) {
            byteBuffer.put(fieldDelimiter.getBytes)
          }

          if (bytes.length == 2 && bytes(0) == '\\'.toByte && bytes(1) == 'N'.toByte) {
            byteBuffer.put(bytes)
          } else {
            byteBuffer.put('"'.toByte)
            bytes.foreach(ch => {
              if (ch == '"'.toByte) {
                byteBuffer.put('"'.toByte).put('"'.toByte)
              } else {
                byteBuffer.put(ch)
              }
            })
            byteBuffer.put('"'.toByte)
          }

          index = index + 1
        })

        byteBuffer.put('\n'.toByte)
        byteBuffer.flip()
        val bytesArray = new Array[Byte](byteBuffer.remaining)
        byteBuffer.get(bytesArray, 0, bytesArray.length)
        bytesArray
      }
    }

    () => if (bytes.hasNext) {
      bytes.next & 0xff // bitwise AND - make the signed byte an unsigned int from 0-255
    } else {
      -1
    }
  }

  def buildUpsertPGSql(tableName: String, tempTableName: String,
                               columns: util.List[String], upsertKeyColumns: Array[String]): String = {

    val updateColumns = columns.asScala.filter(name => !upsertKeyColumns.contains(name)).map(name => name)
    val excludedColumns = updateColumns.map(name => "excluded." + name)

    val sqlBuilder: StringBuilder = new StringBuilder
    sqlBuilder.append("insert into ").append(tableName).append("(").append(StringUtils.join(columns, ",")).append(")\n")
    sqlBuilder.append("select ").append(StringUtils.join(columns, ",")).append("\n")
    sqlBuilder.append("\tfrom ").append(tempTableName).append("\n")
    sqlBuilder.append("on conflict (").append(upsertKeyColumns.mkString(",")).append(")").append("\n")
    sqlBuilder.append("DO UPDATE SET (").append(updateColumns.mkString(",")).append(") = ").append("\n")
    sqlBuilder.append("ROW(").append(excludedColumns.mkString(",")).append(")")
    sqlBuilder.toString
  }

  def copyIn(parameters: Map[String, String])(df: DataFrame, table: String): Unit = {
    df.rdd.foreachPartition { rows =>
      val options = new JdbcOptionsInWrite(parameters)
      val dialect = JdbcDialects.get(options.url)
      val conn = dialect.createConnectionFactory(options)(-1)
      try {
        val cm = new CopyManager(conn.asInstanceOf[BaseConnection])
        val sql = s"COPY $table FROM STDIN WITH (NULL '\\N', FORMAT CSV, DELIMITER E'${fieldDelimiter}')";
        logInfo(s"copy from sql: $sql")
        //LogUtils.info(s"copy from sql: $sql")
        cm.copyIn(sql, rowsToInputStream(rows))
        ()
      } finally {
        JdbcUtils.closeConnection(conn)
      }
    }
  }
}
