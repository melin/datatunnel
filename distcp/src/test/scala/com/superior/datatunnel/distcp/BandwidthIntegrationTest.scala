package com.superior.datatunnel.distcp

import com.superior.datatunnel.distcp.objects.SerializableFileStatus
import com.superior.datatunnel.distcp.utils.CopyUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.junit.{After, Assert, Test}

import java.io.FileOutputStream
import java.net.URI
import java.nio.file.Files
import java.util.concurrent.{Executors, TimeUnit}

class BandwidthIntegrationTest {

  var conf: Configuration = _
  var fs: FileSystem = _
  var tmpDir: java.nio.file.Path = _

  @After
  def cleanup(): Unit = {
    if (fs != null && tmpDir != null) {
      try {
        fs.delete(new Path(tmpDir.toUri), true)
      } catch {
        case _: Throwable => // ignore
      }
    }
  }

  @Test
  def testConcurrentCopiesApproximateBandwidthLimit(): Unit = {
    conf = new Configuration()
    fs = FileSystem.getLocal(conf)
    tmpDir = Files.createTempDirectory("distcp-bandwidth-test")

    val numFiles = 4
    val sizePerFile = 1024 * 1024 // 1 MB
    val totalBytes = numFiles.toLong * sizePerFile

    // create source files with predictable content
    val srcPaths = (0 until numFiles).map { i =>
      val p = tmpDir.resolve(s"src-$i")
      val fos = new FileOutputStream(p.toFile)
      try {
        val buf = new Array[Byte](8192)
        var written = 0
        while (written < sizePerFile) {
          val toWrite = math.min(buf.length, sizePerFile - written)
          fos.write(buf, 0, toWrite)
          written += toWrite
        }
      } finally fos.close()
      new Path(p.toUri)
    }

    val dstPaths = (0 until numFiles).map { i =>
      tmpDir.resolve(s"dst-$i").toUri
    }

    // prepare SerializableFileStatus for each source
    val statusList = srcPaths.map(p => SerializableFileStatus(fs.getFileStatus(p)))

    // set global bandwidth to 1 MB/s
    val bandwidthBytesPerSec = 1024 * 1024L
    CopyUtils.setGlobalBandwidth(bandwidthBytesPerSec)

    val pool = Executors.newFixedThreadPool(numFiles)

    val start = System.nanoTime()

    val futures = (0 until numFiles).map { i =>
      val sfs = statusList(i)
      val dstUri = new URI(dstPaths(i).toString)
      pool.submit(new Runnable {
        override def run(): Unit = {
          // perform copy using the same local FileSystem
          CopyUtils.performCopy(
            fs,
            sfs,
            fs,
            dstUri,
            removeExisting = false,
            ignoreErrors = false,
            taskAttemptID = i,
            options = null
          )
        }
      })
    }

    pool.shutdown()
    val finished = pool.awaitTermination(60, TimeUnit.SECONDS)
    val durationSec = (System.nanoTime() - start).toDouble / 1e9

    Assert.assertTrue("Copy tasks did not finish in time", finished)

    val measuredRate = totalBytes.toDouble / durationSec

    val lower = bandwidthBytesPerSec * 0.7
    val upper = bandwidthBytesPerSec * 1.3

    // Allow some tolerance due to startup/shutdown overhead and buffering
    Assert.assertTrue(
      s"Measured rate $measuredRate B/s not within tolerance of $bandwidthBytesPerSec B/s",
      measuredRate >= lower && measuredRate <= upper
    )
  }
}
