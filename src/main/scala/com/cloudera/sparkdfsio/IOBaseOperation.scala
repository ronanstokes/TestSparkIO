package com.cloudera.sparkdfsio

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkException
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.LongWritable;
import org.apache.spark.storage.StorageLevel
import com.cloudera.utils.Logging
import org.apache.spark.sql.{ SQLContext, Row }
import org.apache.spark.sql.SQLImplicits

abstract class IOBaseOperation(options: Options) extends Logging {
  var writeDir: String = ""

  def computeNumRows() = {
    val td = new TestData(1, 1)
    val tdSize = td.computeBinarySize
    val numRows = Math.floor(options.fileSize / tdSize).toLong

    LOG.info(s"Computing target rows $numRows")
    numRows
  }

  def readControlFile(sc: SparkContext, metrics: ExecMetrics): RDD[(String, Long)] = {
    LOG.info("Readong control files")

    val controlDir = SparkConfigHelper.getControlDir(SparkConfigHelper.currentConf).toString
    LOG.info(s"Readong control files - dir $controlDir")

    val data = sc.sequenceFile(controlDir, classOf[Text], classOf[LongWritable]).map { case (x, y) ⇒ (x.toString, y.get) }
    data.persist(options.storageLevel)

    // force evaluation
    data.foreach(x ⇒ {})

    data
  }

  // set up basic operation - codec etc
  def configure(sparkConf: SparkConf) {

  }

  def collectStats() {

  }

  def exec(sc: SparkContext, metrics: ExecMetrics) {
    LOG.info("Base IOOperation exec")

    // load control data 

    // generate test data

    // sort if required

    // save to 
  }
}

class ReadOperation(options: Options) extends IOBaseOperation(options) {

  def readDataAsSql(sc: SparkContext, metrics: ExecMetrics): RDD[Row] = {
    val dataDir = SparkConfigHelper.getDataDir(SparkConfigHelper.currentConf).toString
    LOG.info("Reading data")

    val sqlContext = new SQLContext(sc)

    val parquetFile = sqlContext.read.parquet(dataDir)

    val data = parquetFile.rdd

    val repartitionedData = if (options.repartition != -1)
      data.repartition(options.repartition) else data

    val coalescedData = if (options.coalesce != -1)
      repartitionedData.coalesce(options.coalesce) else repartitionedData

    coalescedData
  }

  override def exec(sc: SparkContext, metrics: ExecMetrics) {
    val controlData = readControlFile(sc, metrics)

    val tIOStart= System.currentTimeMillis() 

    val testData = readDataAsSql(sc, metrics)
    
    testData.persist(options.storageLevel)

    testData.foreach(x ⇒ {})
   
    metrics.ioExecTime += System.currentTimeMillis() - tIOStart

    metrics.totalRows += testData.count
    metrics.tasks += testData.partitions.size

    
    val tdSize = new TestData(1,2).computeBinarySize
    metrics.totalSize +=  metrics.totalRows.value * tdSize

    
  }
}

class WriteOperation(options: Options) extends IOBaseOperation(options) {

  def generateData(controlData: RDD[(String, Long)], metrics: ExecMetrics): RDD[TestData] = {
    val numRows = computeNumRows
    
    metrics.totalRows += numRows * controlData.partitions.size
    
    val tdSize = new TestData(1,2).computeBinarySize
    metrics.totalSize +=  metrics.totalRows.value * tdSize
    
    val testData = controlData.mapPartitionsWithIndex((x, y) ⇒ new DataGenerator(numRows, 1L), true)
    testData.persist(options.storageLevel)

    val sortedData = if (options.doSort)
      testData.sortBy({ case td: TestData ⇒ td.a })
    else testData

    val repartitionedData = if (options.repartition != -1)
      sortedData.repartition(options.repartition) else sortedData

    val coalescedData = if (options.coalesce != -1)
      repartitionedData.coalesce(options.coalesce) else repartitionedData

    coalescedData
  }

  def save(testData: RDD[TestData], sc: SparkContext, metrics: ExecMetrics): Unit = {
    
    if (options.computeBeforeWrite)
    {
      val tStart = System.currentTimeMillis()

      testData.persist(options.storageLevel)
      testData.foreach( x => {})
      
      metrics.execTime += System.currentTimeMillis() - tStart
    }
    
    val dataDir = SparkConfigHelper.getDataDir(SparkConfigHelper.currentConf).toString
    val tIOStart = System.currentTimeMillis()

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val saveDir = dataDir

    // method 1 - simply save using the parquet standard apis
    testData.toDF.write.parquet(saveDir)
    
    metrics.ioExecTime += System.currentTimeMillis() - tIOStart
    
    metrics.tasks += testData.partitions.size

  }

  override def exec(sc: SparkContext, metrics: ExecMetrics) {
    val controlData = readControlFile(sc, metrics)

    val testData = generateData(controlData, metrics)

    save(testData, sc, metrics)
  }

}

class AppendOperation(options: Options) extends IOBaseOperation(options) {
  override def exec(sc: SparkContext, metrics: ExecMetrics) {
    throw new UnsupportedOperationException("Not yet implemented")
  }
}

class RandomReadOperation(options: Options) extends IOBaseOperation(options) {
  override def exec(sc: SparkContext, metrics: ExecMetrics) {
    throw new UnsupportedOperationException("Not yet implemented")
  }
}

class TruncateOperation(options: Options) extends IOBaseOperation(options) {
  override def exec(sc: SparkContext, metrics: ExecMetrics) {
    throw new UnsupportedOperationException("Not yet implemented")
  }
}