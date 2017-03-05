package com.cloudera.minicluster.tests

import org.scalatest._
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.scalatest.concurrent.Timeouts._
import com.cloudera.sparkdfsio._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.hadoop.conf.Configuration

@RunWith(classOf[JUnitRunner])
class SparkDFSIOTests extends FunSuite with BeforeAndAfter {
	//var  cluster:MiniDFSCluster=null
  val  bench=SparkIO
  private val master = "local[*]"
  private val appName = "example-spark"
  private var sc: SparkContext = _
  var metrics:ExecMetrics = _
  var fs:FileSystem = _

  before {
    //bench.conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1)
    // cluster = new MiniDFSCluster.Builder(bench.conf)
    //    .numDataNodes(2)
    //    .format(true)
   //     .build()
     fs = FileSystem.getLocal(new Configuration()) // cluster.getFileSystem
    
    val conf = new SparkConf()
    populateConf(conf)
    sc = new SparkContext(conf)
    metrics = new ExecMetrics(sc)

    
    bench.createControlFile(fs, bench.DEFAULT_NR_BYTES, bench.DEFAULT_NR_FILES, sc.getConf)

    /** Check write here, as it is required for other tests */
    testWrite()
  }
  
  after {
      
    /*
    if(cluster != null) {
    val fs = cluster.getFileSystem
    bench.cleanup(fs, sc.getConf)
    cluster.shutdown()
    
    * }
    */
    
        if (sc != null)
      sc.stop()

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
  
  def getFS = fs
  
  def testWrite() = {
    bench.validateOptions()
    val fs:FileSystem = getFS
    val metrics = bench.writeTest(fs,sc)
    bench.analyzeResult(fs, TestType.TEST_TYPE_WRITE, metrics)
  }

  test("Read")  {
    bench.validateOptions()
    
    val fs:FileSystem = getFS
    val metrics = bench.readTest(fs,sc)
    bench.analyzeResult(fs, TestType.TEST_TYPE_READ, metrics)
  }

  //@Test (timeout = 10000)
  
  ignore("ReadRandom")  {
    bench.validateOptions()
    val fs:FileSystem = getFS
    bench.mkSparkConfiguration().set("test.io.skip.size", 0.toString)
    
    intercept[java.lang.UnsupportedOperationException] {
    val metrics = bench.randomReadTest(fs,sc)
    bench.analyzeResult(fs, TestType.TEST_TYPE_READ_RANDOM, metrics)
    }
  }

 // @Test (timeout = 10000)
  ignore("ReadBackward" )  {
    bench.validateOptions()
    val fs:FileSystem = getFS
    bench.mkSparkConfiguration().set("test.io.skip.size", (-bench.DEFAULT_BUFFER_SIZE).toString)
    
    intercept[java.lang.UnsupportedOperationException] {
    val metrics = bench.randomReadTest(fs,sc)
    bench.analyzeResult(fs, TestType.TEST_TYPE_READ_BACKWARD, metrics)
    }
  }

 // @Test (timeout = 10000)
  ignore("ReadSkip")  {
    bench.validateOptions()
    val fs:FileSystem = getFS
    bench.mkSparkConfiguration().set("test.io.skip.size", 1.toString)
    intercept[java.lang.UnsupportedOperationException] {
    val metrics= bench.randomReadTest(fs,sc)
    bench.analyzeResult(fs, TestType.TEST_TYPE_READ_SKIP, metrics)
    }
  }



  //@Test (timeout = 60000)
  ignore("Truncate")  {
    bench.validateOptions()
    val fs:FileSystem = getFS
    
        intercept[java.lang.UnsupportedOperationException] {

    bench.createControlFile(fs, bench.DEFAULT_NR_BYTES / 2, bench.DEFAULT_NR_FILES,sc.getConf)
    val metrics = bench.truncateTest(fs,sc)
    bench.analyzeResult(fs, TestType.TEST_TYPE_TRUNCATE, metrics)
    }
  }
  

}