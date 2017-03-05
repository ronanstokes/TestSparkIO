package com.cloudera.general.tests

import org.scalatest._
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import java.io.File
import scala.collection.mutable.ArrayBuffer
import com.cloudera.utils.FileProcessor
import com.cloudera.sparkdfsio._
import org.apache.spark.storage.StorageLevel


@RunWith(classOf[JUnitRunner])
class GeneralTests extends FunSuite with BeforeAndAfter {
  var test1 = 1

  before {
    test1 = 2
  }

  test("testData_basics") {

    val td = TestData(1, "Testing", 2, "Testing again")

    assert(td.computeBinarySize > 0)
    assert(td.computeDisplaySize > 0)

  }

  test("testData_basics2") {

    val td2 = new com.cloudera.sparkdfsio.TestData(1, 1)

    assert(td2.computeBinarySize > 0)
    assert(td2.computeDisplaySize > 0)

  }

  test("testData_generator") {

    val dg = new DataGenerator(100,1)
    
    assert(dg.size == 100)

  }
  
  test("testData_generator2") {

    val dg = new DataGenerator(100,1, true)
    
    assert(dg.size == 100)

  }
  
  test("setting storage levels1") {

    val args=Array("--persist","MEMORY_ONLY")
    
    val options = SparkIO.processOptions(args)
    
    assert(options.storageLevel== StorageLevel.MEMORY_ONLY)
 
  }
  
  test("setting storage levels2") {

    val args=Array("--persist","MEMORY_AND_DISK")
    
    val options = SparkIO.processOptions(args)
    
    assert(options.storageLevel== StorageLevel.MEMORY_AND_DISK)
 
  }
  
  test("setting storage levels3") {

    val args=Array("--persist","MEMORY_AND_DISK_SER")
    
    val options = SparkIO.processOptions(args)
    
    assert(options.storageLevel== StorageLevel.MEMORY_AND_DISK_SER)
 
  }
  
   test("test compute size") {

     val size_4k = SparkIO.parseSizeString("4K")

     assert(size_4k == 4 * 1024L)

          val size_400kb = SparkIO.parseSizeString("400KB")

     assert(size_400kb == 400 * 1024L)

      val size_4m = SparkIO.parseSizeString("4mb")

     assert(size_4m == 4 * 1024 * 1024L)
     
     val size_4g = SparkIO.parseSizeString("4g")

     assert(size_4g == 4 * 1024 * 1024L* 1024)
 
 
  }
   
  test("compression codec settings") (pending)
     
  test("randomize settings") (pending)


}