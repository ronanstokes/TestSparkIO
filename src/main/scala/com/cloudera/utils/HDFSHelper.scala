package com.cloudera.utils

import java.util.UUID

import java.io.File
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.permission.AclStatus
import org.apache.hadoop.fs.permission.FsPermission
import java.io.FileNotFoundException

/**
 * *
 * helper object to apply function to all files in a directory hierarchy
 */
object HDFSHelper extends Logging {

  val defaultConf = new Configuration()
  val defaultPerm = new FsPermission("766")

  def mkTmpDir(pathPrefix: String, createDir: Boolean, conf: Configuration = defaultConf): Path =
    {
      val randomUUID: UUID = UUID.randomUUID
      val sb: StringBuilder = new StringBuilder
      sb ++= ("/tmp/")
      sb ++= (pathPrefix)
      sb ++= ("_")
      sb ++= System.currentTimeMillis().toString
      sb.append("_")
      sb.append(randomUUID.toString().replace("-", ""))
      sb.append("/")

      val tmpDir: Path = new Path(sb.toString())

      if (createDir)
        mkDir(tmpDir, new FsPermission("766"), conf)
      return tmpDir
    }

  def mkDir(p: Path, perm: FsPermission = defaultPerm, conf: Configuration = defaultConf) {
    if (!directoryExists(p, conf)) {
      val fs: FileSystem = getFSForPath(p, conf)
      fs.mkdirs(p, perm)
    }
  }

  def directoryExists(path: Path, conf: Configuration = defaultConf): Boolean =
    {
      if (path == null)
        throw new IllegalArgumentException("no target path specified")

      try {
        val fsDest = getFSForPath(path, conf)
        val fs: FileStatus = fsDest.getFileStatus(path)

        if (!fs.isDirectory())
          throw new IllegalArgumentException("target directory [" + path.toString() + "] does not exist or is not a directory")

        true
      } catch {
        case ex: FileNotFoundException ⇒ {}
      }

      false
    }

  def fileExists(path: Path, conf: Configuration = defaultConf): Boolean =
    {
      if (path == null)
        throw new IllegalArgumentException("no target path specified")

      try {
        val fsDest = getFSForPath(path, conf)
        val fs: FileStatus = fsDest.getFileStatus(path)

        if (!fs.isFile())
          throw new IllegalArgumentException("target directory does not exist or is not a directory")

        return true
      } catch {
        case ex: FileNotFoundException ⇒ {}
      }

      return false
    }

  def getFSForPath(p: Path, conf: Configuration = defaultConf): FileSystem =
    {
      FileSystem.get(p.toUri, conf)
    }

  def isDirectory(filePath: Path, conf: Configuration = defaultConf) = {
    val fsDest = getFSForPath(filePath, conf)
    val fs: FileStatus = fsDest.getFileStatus(filePath)

    fs.isDirectory
  }

  def isFile(filePath: Path, conf: Configuration = defaultConf) = {
    val fsDest = getFSForPath(filePath, conf)
    val fs: FileStatus = fsDest.getFileStatus(filePath)

    fs.isFile
  }

  def mv(filePath: Path, filePath2: Path, conf: Configuration = defaultConf) = {
    val fsSrc = getFSForPath(filePath, conf)
    fsSrc.rename(filePath, filePath2)
  }
}