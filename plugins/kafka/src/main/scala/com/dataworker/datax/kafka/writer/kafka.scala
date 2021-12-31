package com.dataworker.datax.kafka.writer

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

/** Implicit conversions between
 *  - [[DStream]] -> [[KafkaWriter]]
 *  - [[RDD]] -> [[KafkaWriter]]
 *  - [[Dataset]] -> [[KafkaWriter]]
 *  - [[DataFrame]] -> [[KafkaWriter]]
 */
package object kafka {
  /**
   * Convert a [[DStream]] to a [[KafkaWriter]] implicitly
   *
   * @param dStream [[DStream]] to be converted
   * @return [[KafkaWriter]] ready to write messages to Kafka
   */
  implicit def dStreamToKafkaWriter[T: ClassTag, K, V](dStream: DStream[T]): KafkaWriter[T] =
    new DStreamKafkaWriter[T](dStream)

  /**
   * Convert a [[RDD]] to a [[KafkaWriter]] implicitly
   *
   * @param rdd [[RDD]] to be converted
   * @return [[KafkaWriter]] ready to write messages to Kafka
   */
  implicit def rddToKafkaWriter[T: ClassTag, K, V](rdd: RDD[T]): KafkaWriter[T] =
    new RDDKafkaWriter[T](rdd)

  /**
   * Convert a [[Dataset]] to a [[KafkaWriter]] implicitly
   *
   * @param dataset [[Dataset]] to be converted
   * @return [[KafkaWriter]] ready to write messages to Kafka
   */
  implicit def datasetToKafkaWriter[T: ClassTag, K, V](dataset: Dataset[T]): KafkaWriter[T] =
    new DatasetKafkaWriter[T](dataset)

  /**
   * Convert a [[DataFrame]] to a [[KafkaWriter]] implicitly
   *
   * @param dataFrame [[DataFrame]] to be converted
   * @return [[KafkaWriter]] ready to write messages to Kafka
   */
  implicit def datasetToKafkaWriter[K, V](dataFrame: DataFrame): KafkaWriter[Row] =
    new DatasetKafkaWriter[Row](dataFrame)
}
