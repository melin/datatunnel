package com.superior.datatunnel.plugin.jdbc.support

import com.superior.datatunnel.common.util.IOCopier
import org.apache.commons.io.FileUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Row}

import java.io.{File, FileOutputStream}
import java.nio.ByteBuffer
import java.util.UUID

// https://gist.github.com/longcao/bb61f1798ccbbfa4a0d7b76e49982f84
object MysqlSqlHelper extends Logging{

  private val fieldDelimiter = ",";

  def rowsToFile(df: DataFrame, path: String): String = {
    FileUtils.deleteQuietly(new File(path))
    FileUtils.forceMkdir(new File(path))
    df.rdd.foreachPartition { rows =>
      val filePath = path + "/" + UUID.randomUUID().toString + ".csv"
      val fos = new FileOutputStream(filePath)
      writeFile(rows, fos)
      fos.close()
    }

    val list = FileUtils.listFiles(new File(path), Array("csv"), false)
    val files = list.toArray(new Array[File](list.size()))
    logInfo("files: " + files.mkString(","))
    if (files.size == 1) {
      files(0).toPath.toString
    } else {
      val filePath = path + "/total_data.csv"
      IOCopier.joinFiles(new File(filePath), files)
      logInfo("merge file: " + filePath)
      filePath
    }
  }

  private def writeFile(rows: Iterator[Row], fos: FileOutputStream): Unit = {
    rows.foreach {
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
        fos.write(bytesArray)
      }
    }
  }
}
