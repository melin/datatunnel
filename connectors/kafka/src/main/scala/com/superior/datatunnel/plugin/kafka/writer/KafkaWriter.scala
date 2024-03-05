package com.superior.datatunnel.plugin.kafka.writer

import org.apache.kafka.clients.producer.{Callback, ProducerRecord}

import scala.reflect.ClassTag

/**
 * Class used to write DStreams, RDDs and Datasets to Kafka
 *
 * Example usage:
 * {{{
 *   import com.github.benfradet.spark.kafka.writer.KafkaWriter._
 *   import org.apache.kafka.common.serialization.StringSerializer
 *
 *   val topic = "my-topic"
 *   val producerConfig = Map(
 *     "bootstrap.servers" -> "127.0.0.1:9092",
 *     "key.serializer" -> classOf[StringSerializer].getName,
 *     "value.serializer" -> classOf[StringSerializer].getName
 *   )
 *
 *   val dStream: DStream[String] = ...
 *   dStream.writeToKafka(
 *     producerConfig,
 *     s => new ProducerRecord[String, String](topic, s)
 *   )
 *
 *   val rdd: RDD[String] = ...
 *   rdd.writeToKafka(
 *     producerConfig,
 *     s => new ProducerRecord[String, String](localTopic, s)
 *   )
 *
 *   val dataset: Dataset[Foo] = ...
 *   dataset.writeToKafka(
 *     producerConfig,
 *     f => new ProducerRecord[String, String](localTopic, f.toString)
 *   )
 *
 *   val dataFrame: DataFrame = ...
 *   dataFrame.writeToKafka(
 *     producerConfig,
 *     r => new ProducerRecord[String, String](localTopic, r.get(0).toString)
 *   )
 * }}}
 * It is also possible to provide a callback for each write to Kafka.
 *
 * This is optional and has a value of None by default.
 *
 * Example Usage:
 * {{{
 *   @transient val log = org.apache.log4j.Logger.getLogger("spark-kafka-writer")
 *
 *   val dStream: DStream[String] = ...
 *   dStream.writeToKafka(
 *     producerConfig,
 *     s => new ProducerRecord[String, String](topic, s),
 *     Some(new Callback with Serializable {
 *       override def onCompletion(metadata: RecordMetadata, e: Exception): Unit = {
 *         if (Option(e).isDefined) {
 *           log.warn("error sending message", e)
 *         } else {
 *           log.info(s"write succeeded, offset: ${metadata.offset()")
 *         }
 *       }
 *     })
 *   )
 * }}}
 */
abstract class KafkaWriter[T: ClassTag] extends Serializable {
  /**
   * Write a DStream, RDD, or Dataset to Kafka
   * @param producerConfig producer configuration for creating KafkaProducer
   * @param transformFunc a function used to transform values of T type into [[ProducerRecord]]s
   * @param callback an optional [[Callback]] to be called after each write, default value is None.
   */
  def writeToKafka[K, V](
    producerConfig: Map[String, Object],
    transformFunc: T => ProducerRecord[K, V],
    callback: Option[Callback] = None
  ): Unit
}
