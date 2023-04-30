package org.apache.spark.sql.redis

object RedisOptions {
  val REDIS_HOST = "host"
  val REDIS_PORT = "port"
  val REDIS_USER = "user"
  val REDIS_PASSWORD = "password"
  val REDIS_DATABASE = "database"
  val REDIS_TIMEOUT = "timeout"
  val REDIS_SSL_ENABLED = "sslEnabled"

  val REDIS_TABLE = "table"
  val REDIS_KEY_COLUMN = "keyColumn"
  val REDIS_VALUE_COLUMN = "valueColumn"
  val REDIS_TTL = "ttl"
  val REDIS_ITERATOR_GROUPING_SIZE = "iteratorGroupingSize"
  val REDIS_MAX_PIPELINE_SIZE = "maxPipelineSize"

}
