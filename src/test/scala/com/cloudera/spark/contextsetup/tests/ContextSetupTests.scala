package com.cloudera.spark.contextsetup.tests

import org.scalatest._
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import java.io.File
import com.cloudera.utils.FileProcessor
import org.apache.spark.{ SparkConf, SparkContext }
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import org.apache.spark.rdd.RDD
import com.cloudera.utils.Logging

/**
 * *
 * Note : for unit testing Spark jobs, you cannot include avro-tools in your build
 * You must also include a specific version of the JAckson faster xml modules - see the pom.xml for dependencies
 */
@RunWith(classOf[JUnitRunner])
class ContextSetupTests extends FunSuite with BeforeAndAfter with Logging {

  private val master = "local[*]"
  private val appName = "example-spark"

  private var sc: SparkContext = _

  before {
    //System.clearProperty("spark.driver.port")
    //System.clearProperty("spark.hostPort")

    val conf = new SparkConf()
    populateConf(conf)
    sc = new SparkContext(conf)
  }
  
  def populateConf(conf:SparkConf) = {
    conf.setMaster(master)
      .setAppName(appName)
      //.setSparkHome("/Users/rstockes/devTools/spark-1.5.0-bin-hadoop2.6/lib/spark-assembly-1.5.0-hadoop2.6.0.jar")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.io.compression.codec", "lz4") // use lz4 compression code for tests as snappy may not be in path
    // Now it's 24 Mb of buffer by default instead of 0.064 Mb
    //.set("spark.kryoserializer.buffer.mb", "24")
  }

  after {
    if (sc != null) {
      sc.stop()

      //System.clearProperty("spark.driver.port")
      //System.clearProperty("spark.hostPort")

    }
  }

  test("basic spark test") {
    val lines = Seq("To be or not to be.", "That is the question.")
    val rdd = sc.parallelize(lines)
    val counts = rdd.flatMap(line ⇒ line.split(" "))
      .map(word ⇒ (word, 1))
      .reduceByKey(_ + _)

    println("counts: " + counts.collect().toString())
  }

  test("test spark context") {

    val appId = sc.applicationId
    LOG.info(s"application id is : $appId")
  }

 

}