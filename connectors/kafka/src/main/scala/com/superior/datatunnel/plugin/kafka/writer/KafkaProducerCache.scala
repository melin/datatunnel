package com.superior.datatunnel.plugin.kafka.writer

import java.util.concurrent.{Callable, ExecutionException, TimeUnit}

import com.google.common.cache._
import com.google.common.util.concurrent.{ExecutionError, UncheckedExecutionException}
import org.apache.kafka.clients.producer.KafkaProducer

import scala.collection.JavaConverters._
import scala.concurrent.duration._

/** Cache of [[KafkaProducer]]s */
object KafkaProducerCache {
  private type ProducerConf = Seq[(String, Object)]
  private type ExProducer = KafkaProducer[_, _]

  private val removalListener = new RemovalListener[ProducerConf, ExProducer]() {
    override def onRemoval(notif: RemovalNotification[ProducerConf, ExProducer]): Unit =
      notif.getValue.close()
  }

  private val cacheExpireTimeout = 10.minutes.toMillis
  private val cache = CacheBuilder.newBuilder()
    .expireAfterAccess(cacheExpireTimeout, TimeUnit.MILLISECONDS)
    .removalListener(removalListener)
    .build[ProducerConf, ExProducer]()

  /**
   * Retrieve a [[KafkaProducer]] in the cache or create a new one
   *
   * @param producerConfig producer configuration for creating [[KafkaProducer]]
   * @return a [[KafkaProducer]] already in the cache or a new one
   */
  def getProducer[K, V](producerConfig: Map[String, Object]): KafkaProducer[K, V] =
    try {
      cache.get(mapToSeq(producerConfig), new Callable[KafkaProducer[K, V]] {
        override def call(): KafkaProducer[K, V] = new KafkaProducer[K, V](producerConfig.asJava)
      }).asInstanceOf[KafkaProducer[K, V]]
    } catch {
      case e@(_: ExecutionException | _: UncheckedExecutionException | _: ExecutionError)
        if e.getCause != null => throw e.getCause
    }

  /**
   * Flush and close the [[KafkaProducer]] in the cache associated with this config
   *
   * @param producerConfig producer configuration associated to a [[KafkaProducer]]
   */
  def close(producerConfig: Map[String, Object]): Unit = cache.invalidate(mapToSeq(producerConfig))

  private def mapToSeq(m: Map[String, Object]): Seq[(String, Object)] = m.toSeq.sortBy(_._1)
}
