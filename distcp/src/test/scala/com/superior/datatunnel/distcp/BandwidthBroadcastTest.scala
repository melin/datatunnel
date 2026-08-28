package com.superior.datatunnel.distcp

import com.superior.datatunnel.distcp.utils.CopyUtils
import org.apache.spark.sql.SparkSession
import org.junit.{After, Assert, Test}

class BandwidthBroadcastTest {

  var spark: SparkSession = _

  def startSpark(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("BandwidthBroadcastTest")
      .getOrCreate()
  }

  @After
  def stopSpark(): Unit = {
    if (spark != null) spark.stop()
  }

  @Test
  def testBroadcastSetsExecutorBandwidth(): Unit = {
    startSpark()
    val sc = spark.sparkContext

    val bw: Long = 12345L
    val bwBroadcast = sc.broadcast(java.lang.Long.valueOf(bw))

    val rdd = sc.parallelize(1 to 4, 2)

    // Each partition will set the global bandwidth from the broadcast and then return the current global value
    val values = rdd
      .mapPartitions { _ =>
        // set from broadcast (simulating driver->executor broadcast usage)
        CopyUtils.setGlobalBandwidth(bwBroadcast.value)
        Iterator(CopyUtils.getGlobalBandwidth())
      }
      .collect()

    // All returned values should equal the broadcast value
    values.foreach { v =>
      Assert.assertEquals(bw, v.longValue())
    }
  }
}
