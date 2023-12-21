package com.superior.datatunnel.plugin.jdbc.support

import org.apache.spark.sql.execution.datasources.jdbc.JdbcOptionsInWrite
import org.apache.spark.sql.jdbc.JdbcDialects

import java.io.InputStream
import org.apache.spark.sql.{DataFrame, Row}
import org.postgresql.copy.CopyManager
import org.postgresql.core.BaseConnection

object CopyHelper {

  def rowsToInputStream(rows: Iterator[Row]): InputStream = {
    val bytes: Iterator[Byte] = rows.flatMap { row =>
      (row.toSeq
        .map { v =>
          if (v == null) {
            """\N"""
          } else {
            "\"" + v.toString.replaceAll("\"", "\"\"") + "\""
          }
        }
        .mkString("\t") + "\n").getBytes
    }

    new InputStream {
      override def read(): Int =
        if (bytes.hasNext) {
          bytes.next & 0xff // bitwise AND - make the signed byte an unsigned int from 0-255
        } else {
          -1
        }
    }
  }

  def copyIn(parameters: Map[String, String])(df: DataFrame, table: String): Unit = {
    df.rdd.foreachPartition { rows =>
      val options = new JdbcOptionsInWrite(parameters)
      val dialect = JdbcDialects.get(options.url)
      val conn = dialect.createConnectionFactory(options)(-1)
      try {
        val cm = new CopyManager(conn.asInstanceOf[BaseConnection])
        cm.copyIn(
          s"COPY $table " + """FROM STDIN WITH (NULL '\N', FORMAT CSV, DELIMITER E'\t')""",
          rowsToInputStream(rows))
        ()
      } finally {
        conn.close()
      }
    }
  }
}
