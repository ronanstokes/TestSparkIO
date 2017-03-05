package com.cloudera.utils

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * *
 * written by Ronan Stokes, Cloudera
 */

/**
 * *
 * a logging trait to automate the use of apache logging
 */
trait Logging {
  // use inner object to make Logger shared among all instances of type instantiating the  trait
  object Loggable extends Serializable {

    var LOG: Log = null

    def getLog(clazz: Class[_]): Log = {
      if (LOG == null) {
        LOG = LogFactory.getLog(clazz);
      }
      return LOG

    }
  }

  //val LOG = Loggable.LOG
  @transient lazy val LOG: Log = Loggable.getLog(getClass)

  def info(s: String): Unit = if (LOG.isInfoEnabled()) LOG.info(s)
  def warn(s: String): Unit = if (LOG.isInfoEnabled()) LOG.warn(s)
  def error(s: String): Unit = if (LOG.isErrorEnabled()) LOG.error(s)
  // def debug(s: String) = if (LOG.isDebugEnabled()()) LOG.debug(s)

}