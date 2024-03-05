package com.superior.datatunnel.plugin.kafka.writer

import org.apache.kafka.clients.producer.{Callback, ProducerRecord}
import org.apache.spark.sql.Dataset

import scala.reflect.ClassTag

/**
 * Class used for writing [[Dataset]]s to Kafka
 * @param dataset [[Dataset]] to be written to Kafka
 */
class DatasetKafkaWriter[T: ClassTag](@transient private val dataset: Dataset[T])
    extends KafkaWriter[T] with Serializable {
  /**
   * Write a [[Dataset]] to Kafka
   * @param producerConfig producer configuration for creating KafkaProducer
   * @param transformFunc a function used to transform values of T type into [[ProducerRecord]]s
   * @param callback an optional [[Callback]] to be called after each write, default value is None.
   */
  override def writeToKafka[K, V](
    producerConfig: Map[String, Object],
    transformFunc: T => ProducerRecord[K, V],
    callback: Option[Callback] = None
  ): Unit = {
    val rddWriter = new RDDKafkaWriter[T](dataset.rdd)
    rddWriter.writeToKafka(producerConfig, transformFunc, callback)
  }
}
