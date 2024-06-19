package com.superior.datatunnel.plugin.kafka.writer

import org.apache.kafka.clients.producer.{Callback, ProducerRecord}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/** Class used for writing [[RDD]]s to Kafka
  * @param rdd
  *   [[RDD]] to be written to Kafka
  */
class RDDKafkaWriter[T: ClassTag](@transient private val rdd: RDD[T])
    extends KafkaWriter[T]
    with Serializable {

  /** Write a [[RDD]] to Kafka
    * @param producerConfig
    *   producer configuration for creating KafkaProducer
    * @param transformFunc
    *   a function used to transform values of T type into [[ProducerRecord]]s
    * @param callback
    *   an optional [[Callback]] to be called after each write, default value is
    *   None.
    */
  override def writeToKafka[K, V](
      producerConfig: Map[String, Object],
      transformFunc: T => ProducerRecord[K, V],
      callback: Option[Callback] = None
  ): Unit =
    rdd.foreachPartition { partition =>
      val producer = KafkaProducerCache.getProducer[K, V](producerConfig)
      partition
        .map(transformFunc)
        .foreach(record => producer.send(record, callback.orNull))
    }
}
