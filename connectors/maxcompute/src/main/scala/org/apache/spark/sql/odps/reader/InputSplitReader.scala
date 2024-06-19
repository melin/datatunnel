package org.apache.spark.sql.odps.reader

import java.util
import com.aliyun.odps.`type`.TypeInfo
import com.aliyun.odps.cupid.table.v1.Attribute
import com.aliyun.odps.cupid.table.v1.reader.SplitReader
import com.aliyun.odps.data.ArrayRecord
import org.apache.spark.SparkException
import org.apache.spark.sql.odps.converter.TypesConverter
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/** @author
  *   renxiang
  * @date
  *   2021-12-20
  */
class InputSplitReader(
    readSchema: StructType,
    partitionSchema: util.List[Attribute],
    partitionSpec: util.Map[String, String],
    outputDataSchema: List[TypeInfo],
    converters: List[Object => Any],
    recordReader: SplitReader[ArrayRecord]
) extends PartitionReader[InternalRow] {

  private val currentRow = {
    val row = new Array[Any](readSchema.fields.length)
    val offset = outputDataSchema.length
    var i = 0

    while (i < partitionSchema.size()) {
      val attr = partitionSchema.get(i)
      val value = partitionSpec.get(attr.getName)
      val sparkType = TypesConverter.odpsTypeStr2SparkType(attr.getType)

      sparkType match {
        case StringType =>
          row.update(offset + i, UTF8String.fromString(value))
        case LongType =>
          row.update(offset + i, value.toLong)
        case IntegerType =>
          row.update(offset + i, value.toInt)
        case ShortType =>
          row.update(offset + i, value.toShort)
        case ByteType =>
          row.update(offset + i, value.toByte)
        case dt: DataType =>
          throw new SparkException(
            s"Unsupported partition column type: ${dt.simpleString}"
          )
      }
      i += 1
    }

    row
  }

  override final def next: Boolean = recordReader.hasNext

  override final def get(): InternalRow = {
    val record = recordReader.next()
    var i = 0
    if (record ne null) {
      while (i < converters.length) {
        val value = record.get(i)
        if (value ne null) {
          currentRow.update(i, converters(i)(value))
        } else {
          currentRow.update(i, null)
        }
        i += 1
      }
    } else {
      while (i < converters.length) {
        currentRow.update(i, null)
        i += 1
      }
    }

    val dataRow = new Array[Any](currentRow.length)
    currentRow.copyToArray(dataRow)
    new GenericInternalRow(dataRow)
  }

  override final def close(): Unit = recordReader.close()
}
