/**
 * Copyright (c) 2016-2017, Benjamin Fradet, and other contributors.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
