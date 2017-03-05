package com.cloudera.utils

// code from open source project 
// see - https://github.com/ippontech/spark-kafka-source/blob/master/src/main/scala/com/ippontech/kafka/util/Stopwatch.scala

// very simple stop watch to avoid using Guava's one
class Stopwatch {
  private val start = System.currentTimeMillis()
  private var end:Long = 0
  
  override def toString() = (System.currentTimeMillis() - start) + " ms"

}