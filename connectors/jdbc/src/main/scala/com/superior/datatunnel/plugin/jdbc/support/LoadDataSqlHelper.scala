package com.superior.datatunnel.plugin.jdbc.support

import com.gitee.melin.bee.util.JdbcUtils
import com.mysql.cj.jdbc.JdbcStatement
import com.oceanbase.jdbc.OceanBaseStatement
import com.superior.datatunnel.api.DataTunnelException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.jdbc.JdbcOptionsInWrite
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.{DataFrame, Row}

import java.io.InputStream
import java.nio.ByteBuffer

// https://gist.github.com/longcao/bb61f1798ccbbfa4a0d7b76e49982f84
object LoadDataSqlHelper extends Logging{

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
                byteBuffer.put('\\'.toByte).put('"'.toByte)
              } else if (ch == '\\') {
                byteBuffer.put('\\'.toByte).put(ch)
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

  def loadData(dataSourceType: String, parameters: Map[String, String])(df: DataFrame, loadCommand: String): Unit = {
    df.rdd.foreachPartition { rows =>
      val options = new JdbcOptionsInWrite(parameters)
      val dialect = JdbcDialects.get(options.url)
      val conn = dialect.createConnectionFactory(options)(-1)
      val statement = conn.createStatement();
      try {
        if ("MYSQL".equalsIgnoreCase(dataSourceType)) {
          val jdbcStatement = statement.asInstanceOf[JdbcStatement];
          jdbcStatement.setLocalInfileInputStream(rowsToInputStream(rows))
        } else if ("OCEANBASE".equalsIgnoreCase(dataSourceType)) {
          val jdbcStatement = statement.asInstanceOf[OceanBaseStatement];
          jdbcStatement.setLocalInfileInputStream(rowsToInputStream(rows))
        } else {
          throw new DataTunnelException(s"$dataSourceType not support load data")
        }

        statement.execute(loadCommand)
      } finally {
        JdbcUtils.closeStatement(statement)
        JdbcUtils.closeConnection(conn)
      }
    }
  }
}
