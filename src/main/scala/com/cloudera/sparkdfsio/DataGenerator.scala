package com.cloudera.sparkdfsio

import scala.util.Random

 case class TestData (a:Long, b:String, c:Long, d:String) {
    require(b != null)
    require(d != null)
    
    def this(a:Long, c:Long) = this(a, "Key [%-20d]" format a,c, "Iteration [%-20d]" format c)
    
    def computeDisplaySize = 19 + 19 +b.size + d.size
    
    def computeBinarySize = 8 + 8 +b.size + d.size
  }
 
  object DataGenerator {
    val rnd = new Random
    
    def nextLong(limit:Long, start:Long) = Math.floor(rnd.nextDouble * limit).toLong  + start
  }
 
  class DataGenerator( numElements:Long, startKey:Long, useRandom:Boolean = false) extends Iterator[TestData] {
    var i:Long= 0
    var key = startKey
    
    def hasNext = i < numElements
    def next:TestData = 
    { 
      val newData = if (useRandom) 
        new TestData(DataGenerator.nextLong(numElements, startKey), i)
      else 
        new TestData(key, i)
      key += 1
      i += 1
      newData

    }
  }