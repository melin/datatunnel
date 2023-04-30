package org.apache.spark.sql.redis

import com.superior.datatunnel.plugin.redis.{RedisConfig, RedisNode}

import java.util.UUID
import org.apache.spark.sql.redis.RedisSourceRelation._
import org.apache.spark.sql.sources.{BaseRelation, Filter, InsertableRelation}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.redis.RedisOptions._
import redis.clients.jedis.{Jedis, Pipeline}
import redis.clients.jedis.util.JedisClusterCRC16

import scala.collection.TraversableOnce

class RedisSourceRelation(override val sqlContext: SQLContext,
                          parameters: Map[String, String])
  extends BaseRelation
    with InsertableRelation
    with Serializable
    with Logging {

  private val redisConfig: RedisConfig = RedisConfig.fromRedisOptions(parameters)

  logInfo(s"Redis config initial host: ${redisConfig.initialHost}")

  @transient private val sc = sqlContext.sparkContext

  private val iteratorGroupingSize = parameters.get(REDIS_ITERATOR_GROUPING_SIZE).map(_.toInt).getOrElse(100)
  private val keyColumn = parameters.get(REDIS_KEY_COLUMN)
  private val valueColumn = parameters.get(REDIS_VALUE_COLUMN)
  private val tableNameOpt: Option[String] = parameters.get(REDIS_TABLE)
  private val maxPipelineSize: Int = parameters.get(REDIS_MAX_PIPELINE_SIZE).map(_.toInt).getOrElse(100)
  private val ttl = parameters.get(REDIS_TTL).map(_.toInt).getOrElse(0)

  override def schema: StructType = {
    StructType(StructField("key", StringType)::
        StructField("json", StringType)::Nil)
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    // write data
    data.foreachPartition { partition: Iterator[Row] =>
      // grouped iterator to only allocate memory for a portion of rows
      partition.grouped(iteratorGroupingSize).foreach { batch =>
        // the following can be optimized to not create a map
        val rowsWithKey: Map[String, Row] = batch.map(row => dataKeyId(row) -> row).toMap
        groupKeysByNode(redisConfig.hosts, rowsWithKey.keysIterator).foreach { case (node, keys) =>
          val conn = node.connect()
          foreachWithPipeline(conn, maxPipelineSize, keys) { (pipeline, key) =>
            val row = rowsWithKey(key)
            val encodedRow = if (valueColumn.isDefined) {
              row.json
            } else {
              row.getAs(valueColumn.get).toString
            }
            save(pipeline, key, encodedRow, ttl)
          }
          conn.close()
        }
      }
    }
    // If dataframe contains only key column
    val colCount = data.columns.length
    if (colCount < 2) {
      logInfo(s"Dataframe only contains key.column specified in options. No data was writen to redis.")
    }
  }

  private def save(pipeline: Pipeline, key: String, value: String, ttl: Int): Unit = {
    pipeline.set(key, value)
    if (ttl > 0) {
      pipeline.expire(key, ttl.toLong)
    }
  }

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = filters

  /**
   * @return table name
   */
  private def tableName(): String = {
    tableNameOpt.getOrElse(throw new IllegalArgumentException(s"Option '$REDIS_TABLE' is not set."))
  }

  /**
   * @return redis key for the row
   */
  private def dataKeyId(row: Row): String = {
    val id = keyColumn.map(id => row.getAs[Any](id)).map(_.toString)
      .getOrElse(throw new IllegalArgumentException(s"key column ${keyColumn.get} not exists"))
    dataKey(tableName(), id)
  }
}

object RedisSourceRelation {

  def dataKey(tableName: String, id: String = uuid()): String = {
    s"$tableName:$id"
  }

  def uuid(): String = UUID.randomUUID().toString.replace("-", "")

  /**
   * Master node for a key
   *
   * @param nodes list of all nodes
   * @param key   key
   * @return master node
   */
  def getMasterNode(nodes: Array[RedisNode], key: String): RedisNode = {
    val slot = JedisClusterCRC16.getSlot(key)
    /* Master only */
    nodes.filter { node => node.startSlot <= slot && node.endSlot >= slot }.filter(_.idx == 0)(0)
  }

  /**
   * @param nodes list of RedisNode
   * @param keys  list of keys
   * @return (node: (key1, key2, ...), node2: (key3, key4,...), ...)
   */
  def groupKeysByNode(nodes: Array[RedisNode], keys: Iterator[String]): Iterator[(RedisNode, Array[String])] = {
    keys.map(key => (getMasterNode(nodes, key), key)).toArray.groupBy(_._1).
      map(x => (x._1, x._2.map(_._2))).iterator
  }

  /**
   * Executes a pipeline function for each item in the sequence. No response is returned.
   *
   * Ensures that a new pipeline is created if the number of operations exceeds the given maxPipelineSize
   * while iterating over the items.
   *
   * @param conn            jedis connection
   * @param items           a sequence of elements (typically keys)
   * @param f               function to applied for each item in the sequence
   */
  def foreachWithPipeline[A](conn: Jedis, maxPipelineSize: Int, items: TraversableOnce[A])(f: (Pipeline, A) => Unit): Unit = {
    // iterate over items and create new pipelines periodically
    var i = 0
    var pipeline = conn.pipelined()
    for (x <- items) {
      f(pipeline, x)
      i = i + 1
      if (i % maxPipelineSize == 0) {
        pipeline.sync()
        pipeline = conn.pipelined()
      }
    }

    // sync remaining items
    if (i % maxPipelineSize != 0) {
      pipeline.sync()
    }
  }
}