package com.superior.datatunnel.plugin.kafka.writer

import org.apache.kafka.clients.producer.{Callback, ProducerRecord}
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

/**
 * Class used for writing [[DStream]]s to Kafka
 * @param dStream [[DStream]] to be written to Kafka
 */
class DStreamKafkaWriter[T: ClassTag](@transient private val dStream: DStream[T])
    extends KafkaWriter[T] with Serializable {
  /**
   * Write a [[DStream]] to Kafka
   * @param producerConfig producer configuration for creating KafkaProducer
   * @param transformFunc a function used to transform values of T type into [[ProducerRecord]]s
   * @param callback an optional [[Callback]] to be called after each write, default value is None.
   */
  override def writeToKafka[K, V](
    producerConfig: Map[String, Object],
    transformFunc: T => ProducerRecord[K, V],
    callback: Option[Callback] = None
  ): Unit =
    dStream.foreachRDD { rdd =>
      val rddWriter = new RDDKafkaWriter[T](rdd)
      rddWriter.writeToKafka(producerConfig, transformFunc, callback)
    }
}
