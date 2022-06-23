package com.superior.datatunnel.sftp.spark

import org.apache.commons.io.FileUtils
import org.apache.log4j.Logger

import java.io.File

/**
 * Delete the temp file created during com.dataworker.datax.sftp.spark shutdown
 */
class DeleteTempFileShutdownHook(
                                  fileLocation: String) extends Thread {

  private val logger = Logger.getLogger(classOf[DatasetRelation])

  override def run(): Unit = {
    logger.info("Deleting " + fileLocation )
    FileUtils.deleteQuietly(new File(fileLocation))
  }
}
