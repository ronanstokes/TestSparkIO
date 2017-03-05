package com.cloudera.utils

import java.io.File

/**
 * *
 * helper object to apply function to all files in a directory hierarchy
 */
object FileProcessor extends Logging {

  /**
   * process each file apply function fp to file
   */
  def processFiles(sourceDataFolder: String, fp: File ⇒ Unit): Unit = {
    val dataFolder = new File(sourceDataFolder)
    if (dataFolder.isDirectory) {
      val files = dataFolder.listFiles().iterator
      files.foreach(f ⇒ {
        LOG.info("--Input:" + f)
        if (fp != null) fp(f)
      })
    } else {
      LOG.info("--Input:" + dataFolder)
      if (fp != null) fp(dataFolder)
    }
  }

}