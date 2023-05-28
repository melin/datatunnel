package com.superior.datatunnel.plugin.s3.spark

import org.apache.commons.io.FileUtils
import org.slf4j.LoggerFactory

import java.io.File

/**
 * Delete the temp file created during com.dataworker.datax.sftp.spark shutdown
 */
class DeleteTempFileShutdownHook(
                                  fileLocation: String) extends Thread {

  private val logger = LoggerFactory.getLogger(classOf[DatasetRelation])

  override def run(): Unit = {
    logger.info("Deleting " + fileLocation )
    FileUtils.deleteQuietly(new File(fileLocation))
  }
}
