package com.cloudera.sparkdfsio

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkException
import org.apache.hadoop.conf.Configuration

import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.permission.AclStatus
import org.apache.hadoop.fs.permission.FsPermission

import com.cloudera.utils.Logging

import org.apache.spark.Accumulator
import org.apache.spark.broadcast.Broadcast
import com.cloudera.utils.HDFSHelper
import scala.collection.mutable.ArrayBuffer
import java.io.{ PrintWriter, File, FileInputStream, InputStream, PrintStream, OutputStream }
import java.io.{ BufferedReader, InputStreamReader, IOException, DataInputStream, FileOutputStream }
import collection.JavaConverters._
import java.util.Map.Entry
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.LongWritable;
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkEnv
import java.text.DecimalFormat
import java.util.Date

/*

TestSparkDFSIO - port of classic map-reduce test to spark 

With enhancements for 

- write to Parquet using multiple methods
- coalescing to reduced set of partitions before write to parquet

 */

case class ExecMetrics(@transient sc: SparkContext) {
  var totalSize: Accumulator[Long] = sc.accumulator(0L, "totalSize")
  var totalRows: Accumulator[Long] = sc.accumulator(0L, "totalRows")
  var execTime: Accumulator[Long] = sc.accumulator(0L, "execTime")
  var ioExecTime: Accumulator[Long] = sc.accumulator(0L, "ioExecTime")
  var tasks: Accumulator[Long] = sc.accumulator(0L, "tasks")

  def ioRateMBSec = totalSize.value.toDouble * 1000 / (ioExecTime.value * Multipliers.BYTE_MULTIPLE_MB);
  def rate = ioRateMBSec * 1000
  def sqrate = ioRateMBSec * ioRateMBSec * 1000
  var externalExecTime: Long = 0
}

case class Options() {
  
  final val PARQUET_DATA="parquet"
  final val SEQUENCE_FILE_DATA="sequenceFile"
  
  var outputCodec: String = "none"
  var nrFiles: Int = 100
  var fileSize: Long = 1000000
  var coalesce: Int = -1
  var repartition: Int = -1
  var skipSize: Long = 0
  var bufferSize: Long = SparkIO.DEFAULT_BUFFER_SIZE
  var parquetBlockSize: Long = 128 * Multipliers.BYTE_MULTIPLE_MB
  var parquetPageSize: Long = 1 * Multipliers.BYTE_MULTIPLE_MB
  var parquetDictionaryPageSize:Long = 1 * Multipliers.BYTE_MULTIPLE_MB
  var parquetDirect:Boolean = false
  var doSort: Boolean = false
  var enableDictionaryEncoding = false
  var summaryMetadata = false
  var parquetSchemaMerge = false
  var isSequential: Boolean = false
  var testType: String = TestType.TEST_TYPE_WRITE
  var storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_2
  var randomize = false
  var useSQLIO = false
  var dataFormat = PARQUET_DATA
  var computeBeforeWrite = false
}

object SparkConfigHelper {
  final def getBaseDir(conf: SparkConf): String = conf.get("test.build.data", "/benchmarks/TestDFSIO")

  final def getControlDir(conf: SparkConf): Path = new Path(getBaseDir(conf), "io_control")

  final def getWriteDir(conf: SparkConf): Path = new Path(getBaseDir(conf), "io_write")

  final def getReadDir(conf: SparkConf): Path = new Path(getBaseDir(conf), "io_read")

  final def getAppendDir(conf: SparkConf): Path = new Path(getBaseDir(conf), "io_append")

  final def getRandomReadDir(conf: SparkConf): Path = new Path(getBaseDir(conf), "io_random_read")

  final def getTruncateDir(conf: SparkConf): Path = new Path(getBaseDir(conf), "io_truncate")

  final def getDataDir(conf: SparkConf): Path = new Path(getBaseDir(conf), "io_data")

  final def currentConf = SparkEnv.get.conf

}

/**
 * *
 * MobileDataStreamer stream implements the HBase based enrichment process
 */
object SparkIO extends Logging {

  // constants

  final val DEFAULT_BUFFER_SIZE = 1000000
  final val BASE_FILE_NAME = "test_io_"
  final val DEFAULT_RES_FILE_NAME = "TestDFSIO_results.log"
  final val MEGA = Multipliers.BYTE_MULTIPLE_MB
  final val DEFAULT_NR_BYTES = 128
  final val DEFAULT_NR_FILES = 4

  var hadoopConf:Configuration = new Configuration()
  var sparkConf:SparkConf = new SparkConf
  var options = new Options

  @transient var additionalSparkProperties = new Properties()

  // key options for optimization are 
  // parquet direct commiter output

  // manually managing aparquet metadata avoids a lot of overhead - this is fixed in Spark 2.0
  // sc.hadoopConfiguration.set("parquet.metadata.read.parallelism","10")
  // sc.hadoopConfiguration.set("parquet.enable.summary-metadata","false")

  final val usage = s""" Usage: ${SparkIO.getClass.getSimpleName}
               [genericOptions]
               -read [-random* | -backward* | -skip* [-skipSize* Size]] | 
               --write | -append* | -truncate* | -clean 
               --persist [storageLevel]
               [--compute-before-write]
               [--compression codecClassName]
               [--snappy*] [--gzip*]
               [--nrFiles N] 
               [-size Size[B|KB|MB|GB|TB]] 
               [-resFile resultFileName] [-bufferSize Bytes]
               [ --sequence_file* ]
               [--parquet] [--parquetBlockSize N] [ --parquetPageSize N] [ --parquetDictionaryPageSize N]
               [--parquetDirect]
               [--parquetStream][--parquetSqlWrite]
               [--parquetDisableSummaryMetadata][--parquetEnableSummaryMetadata]
               [--parquetDisableSchemaMerge][--parquetEnableSchemaMerge]
               [--coalesce N]
               
               Note functionality marked with (*) is not yet implemented
  """

  // reset methods to reset for testng purposes
  def resetOptions() { options = new Options() }
  def resetSparkConf() { sparkConf = new SparkConf() }
  def resetHadoopConf() { hadoopConf = new Configuration() }
  
  /**
   * *
   * parse size strin
   */
  def parseSizeString(sSize: String): Long = {
    require(sSize != null)
    require(!sSize.isEmpty)

    val sizeSpec="""([0-9]+)([a-zA-Z]+)""".r
    
    val result = sSize.toLowerCase match {
      case sizeSpec(x,s) if (s =="k") || (s == "kb") => x.toLong * Multipliers.BYTE_MULTIPLE_KB
      case sizeSpec(x,s) if (s =="m") || (s == "mb") => x.toLong * Multipliers.BYTE_MULTIPLE_MB
      case sizeSpec(x,s) if (s =="g") || (s == "gb") => x.toLong * Multipliers.BYTE_MULTIPLE_GB
      case sizeSpec(x,s) if (s =="t") || (s == "tb") => x.toLong * Multipliers.BYTE_MULTIPLE_TB
      case sizeSpec(x,s) if (s =="b") => x.toLong * Multipliers.BYTE_MULTIPLE_B
      case _ => throw new IllegalArgumentException("Unsupported ByteMultiple " + sSize)
    }
    result
  }

  /**
   * *
   * Spark alternative is
   * val data = sc.parallelize( ( 1 to nrFiles),nrFiles)
   * data.map( x => (getFileName(u), x.toLong)).saveAsSequenceFile("/tmp/spark1/output")
   */
  def createControlFile(fs: FileSystem,
                        nrBytes: Long, // in bytes
                        nrFiles: Int,
                        conf: SparkConf): Unit = {
    LOG.info("creating control file: " + nrBytes + " bytes, " + nrFiles + " files")

    val controlDir = SparkConfigHelper.getControlDir(conf)
    val hConf = new Configuration
    fs.delete(controlDir, true)

    for (i ← 0 until nrFiles) {
      val name = getFileName(i)
      val controlFile = new Path(controlDir, "in_file_" + name)
      var writer: SequenceFile.Writer = null
      try {
        writer = SequenceFile.createWriter(fs, hConf, controlFile,
          classOf[Text], classOf[LongWritable],
          CompressionType.NONE)
        writer.append(new Text(name), new LongWritable(nrBytes))
      } catch {
        case e: Exception ⇒
          throw new IOException(e.getLocalizedMessage())
      } finally {
        if (writer != null)
          writer.close()
        writer = null
      }
    }
    LOG.info("created control files for: " + nrFiles + " files")
  }

  def getFileName(fIdx: Long) = BASE_FILE_NAME + fIdx.toString

  /**
   * *
   * validate param
   */
  def validateParam(s: String, arg: String, msg: String) = {
    require(arg != null, "must have parameter validation name")
    require(msg != null, "must have parameter validation message")

    LOG.info(s"validating parameters $arg")

    if (s == null || s.isEmpty()) flagParamError(s"parameter error for $arg - $msg")
  }

  def getBoolArg(args: Array[String], argNum: Int) = {
    if ((argNum + 1 < args.size) && ((args(argNum + 1) == "false") || (args(argNum + 1) == "true")))
      args(argNum + 1).toBoolean
    else
      true
  }

  def getBoolArgNum(args: Array[String], argNum: Int) = {
    if ((argNum + 1 < args.size) && ((args(argNum + 1) == "false") || (args(argNum + 1) == "true")))
      2
    else
      1
  }

  /**
   * *
   * process the command line options
   */
  def processOptions(args: Array[String]): Options = {
    val options = new Options
    if (args.length == 0) {
      println(usage)
    } else {

      LOG.info("processing options")

      var argNum = 0

      // process options
      var optionProcessed = false

      do {
        optionProcessed = true
        val option = args(argNum)
        val sparkProp2 = """(spark\.[a-zA-Z\.0-9_\-]+)=(.*)""".r
        val sparkProp1 = """(spark\.[a-zA-Z\.0-9_\-]+)""".r

        option match {

          case "--read" ⇒
            options.testType = TestType.TEST_TYPE_READ

          case "--write" ⇒
            options.testType = TestType.TEST_TYPE_WRITE

          case "--append" ⇒
            options.testType = TestType.TEST_TYPE_APPEND

          case "--random" ⇒
            options.testType = TestType.TEST_TYPE_READ_RANDOM

          case "--seq" ⇒
            options.isSequential = true

          case "--backward" ⇒
            options.testType = TestType.TEST_TYPE_READ_BACKWARD

          case "--skip" ⇒
            options.testType = TestType.TEST_TYPE_READ_SKIP

          case "--truncate" ⇒
            options.testType = TestType.TEST_TYPE_TRUNCATE

          case "--clean" ⇒
            options.testType = TestType.TEST_TYPE_CLEANUP

          case "--useSparkSQL" ⇒
            options.useSQLIO = true

          case "--parquet" => options.dataFormat =options.PARQUET_DATA
          case "--sequence-file" => options.dataFormat =options.SEQUENCE_FILE_DATA

          case "--compute-before-write" => options.computeBeforeWrite =true
          
          case "--parquetDisableSummaryMetadata" => options.summaryMetadata =false
          case "--parquetEnableSummaryMetadata" => options.summaryMetadata =true
          case "--parquetDisableSchemeMerge" => options.parquetSchemaMerge =false
          case "--parquetEnableSchemaMerge" => options.parquetSchemaMerge =true
          
          case "--parquetDirect" => options.parquetDirect = true
          
          case "--sort"             ⇒ options.doSort = true

          case "--skipSize"         ⇒ { options.skipSize = parseSizeString(args(argNum + 1)); argNum += 1 }
          case "--bufferSize"       ⇒ { options.bufferSize = parseSizeString(args(argNum + 1)); argNum += 1 }
          case "--parquetBlockSize" ⇒ { options.parquetBlockSize = parseSizeString(args(argNum + 1)); argNum += 1 }
          case "--parquetPageSize"  ⇒ { options.parquetPageSize = parseSizeString(args(argNum + 1)); argNum += 1 }
          case "--parquetDictionaryPageSize"  ⇒ { options.parquetDictionaryPageSize = parseSizeString(args(argNum + 1)); argNum += 1 }
          case "--compression"      ⇒ { options.outputCodec = args(argNum + 1); argNum += 1 }
          case "--coalesce"         ⇒ { options.coalesce = args(argNum + 1).toInt; argNum += 1 }
          case "--repartition"      ⇒ { options.repartition = args(argNum + 1).toInt; argNum += 1 }
          case "--persist" ⇒ {
            try {
              val storageSpec = args(argNum + 1)
              require((storageSpec != null) && !storageSpec.isEmpty, "Storage level must be valid storage level")

              options.storageLevel = StorageLevel.fromString(storageSpec)
            } catch {
              case e: Exception ⇒ throw new SparkException("Invalid storage level spec", e)
            }
            argNum += 1
          }

          case "--config" ⇒
            {
              LOG.info("processing option :config")

              val newConfigFile = args(argNum + 1)
              parseNewConfigFile(newConfigFile)
              argNum += 1
            }

          case "--nrFiles" ⇒
            {
              options.nrFiles = args(argNum + 1).toInt
              argNum += 1
            }

          case "--size" ⇒
            {
              options.nrFiles = parseSizeString(args(argNum + 1)).toInt
              argNum += 1
            }

          case sparkProp1(p) ⇒ {
            val v = args(argNum + 1)
            LOG.info(s"processing Spark option :($p) $v")
            additionalSparkProperties.put(p, v)
            argNum += 1
          }
          case sparkProp2(p, v) ⇒ {
            LOG.info(s"processing Spark option :($p=$v)")
            additionalSparkProperties.put(p, v)
          }
          case _ ⇒ {
            LOG.warn(s"unrecognized option: $option")
            optionProcessed = true
          }
        }

        argNum += 1
      } while (optionProcessed && argNum < args.length);

      LOG.info("processing options - complete")

    }
    
    // fix up options
    if (options.testType == TestType.TEST_TYPE_READ_BACKWARD)
      options.skipSize = -options.bufferSize
    else if (options.testType == TestType.TEST_TYPE_READ_SKIP && options.skipSize == 0)
      options.skipSize = options.bufferSize

    LOG.info(s"""
     nrFiles =${options.nrFiles}
     nrBytes (MB) = ${toMB(options.fileSize)}
     bufferSize = ${options.bufferSize}
    """)

    if (options.skipSize > 0) {
      LOG.info("skipSize = " + options.skipSize);
    }

    options
  }

  /**
   * *
   * parse the configuration file -
   * using processOptions to parse the settings
   * this means that
   */
  def parseNewConfigFile(fileName: String) = {
    val newProperties = new Properties()
    var is: InputStream = null
    try {
      LOG.info(s"Loading configuration properties from file : $fileName")
      is = new FileInputStream(fileName)
      newProperties.load(is)

      // convert properties to array 
      val propStrings = new ArrayBuffer[String]()

      newProperties.entrySet.asScala.foreach(
        entry ⇒ {
          propStrings += entry.getKey.toString
          propStrings += entry.getValue.toString
        })
      LOG.info(s"Finished Loading configuration properties from file : $fileName")

      processOptions(propStrings.toArray)
      LOG.info(s"Finished processing configuration properties from file : $fileName")

    } finally {
      if (is != null)
        is.close
    }
  }

  /**
   * *
   * log error and throw exception
   */
  def flagParamError(s: String) {
    LOG.error(s)
    throw new SparkException(s)
  }

  /**
   * *
   * validate options thowing exceptions if an issue occurs
   */
  def validateOptions() = {
    LOG.info("validating options")

    require(options != null)
    require(options.nrFiles > 0, "Number of files must be specified")
    require(options.fileSize > 0, "Size of files must be specified")
    
    require(options.outputCodec != null , "compression codec must be empty string or valid codec")
    require(options.outputCodec != null , "compression codec must be empty string or valid codec")
    
    val codecs = List("none","snappy", "gzip","lz4")
    require(codecs.contains(options.outputCodec ) , s"""compression codec must be one of [${codecs.mkString(",")}]""")
    
    require(options.parquetPageSize <= options.parquetBlockSize  , "parquet page size should be less than parquet block size")


    LOG.info("options : " + options.toString)

  }

  /**
   * *
   * run checks before main streaming process begines
   */
  def runPreStartChecks() {

    LOG.info("Running pre-start checks")

    LOG.info("Checking HDFS outputs folder")
    val baseDir = SparkConfigHelper.getBaseDir(sparkConf)
    if (HDFSHelper.directoryExists(new Path(baseDir))) {
      LOG.error(s"Benchmark folder [$baseDir] already exist")
      throw new SparkException(s"Benchmark folder [$baseDir] already exists")
    }
  }

  def logSparkConfiguration(conf: SparkConf) = {
    val sb = new StringBuffer
    conf.getAll.foreach {
      case (k, v) ⇒
        sb.append("|    %s=%s\n" format (k, v))
    }

    LOG.info(s"""|
                 |  Spark Configuration configuration:
                 |${sb.toString.stripMargin('|')}""".stripMargin('|'))
  }

  def writeTest(fs: FileSystem, sc: SparkContext): ExecMetrics = {
    val execMetrics = new ExecMetrics(sc)
    val writeDir = SparkConfigHelper.getWriteDir(sc.getConf)
    fs.delete(SparkConfigHelper.getDataDir(sc.getConf), true)
    fs.delete(writeDir, true)
    val tStart = System.currentTimeMillis()
    runIOTest(new WriteOperation(options), writeDir, sc, execMetrics)
    execMetrics.externalExecTime += System.currentTimeMillis() - tStart
    execMetrics
  }

  def runIOTest(ioOperation: IOBaseOperation, outputDir: Path, sc: SparkContext, metrics: ExecMetrics): Unit = {
    ioOperation.writeDir = outputDir.toString
    ioOperation.exec(sc, metrics)
  }

  def truncateTest(fs: FileSystem, sc: SparkContext): ExecMetrics = {
    val execMetrics = new ExecMetrics(sc)
    val TruncateDir = SparkConfigHelper.getTruncateDir(sc.getConf)
    fs.delete(TruncateDir, true)
    val tStart = System.currentTimeMillis()
    runIOTest(new TruncateOperation(options), TruncateDir, sc, execMetrics)
    execMetrics.externalExecTime += System.currentTimeMillis() - tStart
    execMetrics
  }

  def sequentialTest(fs: FileSystem,
                     testType: String,
                     fileSize: Long, // in bytes
                     nrFiles: Int,
                     sc: SparkContext,
                     metrics: ExecMetrics): Unit = {
    var ioer: IOBaseOperation = null
    testType match {
      case TestType.TEST_TYPE_READ ⇒
        ioer = new ReadOperation(options)

      case TestType.TEST_TYPE_WRITE ⇒
        ioer = new WriteOperation(options)

      case TestType.TEST_TYPE_APPEND ⇒
        ioer = new AppendOperation(options)

      case TestType.TEST_TYPE_READ_RANDOM   ⇒ ioer = new RandomReadOperation(options)
      case TestType.TEST_TYPE_READ_BACKWARD ⇒ ioer = new RandomReadOperation(options)
      case TestType.TEST_TYPE_READ_SKIP ⇒
        ioer = new RandomReadOperation(options)
        ioer = new RandomReadOperation(options)
      case TestType.TEST_TYPE_TRUNCATE ⇒
        ioer = new TruncateOperation(options)

      case _ ⇒ {}
    }

    if (ioer != null) {
      /*
    for(i <- 0 until nrFiles)
      ioer.exec(Reporter.NULL,
                BASE_FILE_NAME+Integer.toString(i), 
                fileSize)
                * 
                */
    }
  }

  def appendTest(fs: FileSystem, sc: SparkContext): ExecMetrics = {
    val execMetrics = new ExecMetrics(sc)

    val appendDir = SparkConfigHelper.getAppendDir(sc.getConf)
    fs.delete(appendDir, true)
    val tStart = System.currentTimeMillis()
    runIOTest(new AppendOperation(options), appendDir, sc, execMetrics)
    execMetrics.externalExecTime += System.currentTimeMillis() - tStart
    execMetrics
  }

  def readTest(fs: FileSystem, sc: SparkContext): ExecMetrics = {
    val execMetrics = new ExecMetrics(sc)

    val readDir = SparkConfigHelper.getReadDir(sc.getConf)
    fs.delete(readDir, true)
    val tStart = System.currentTimeMillis()
    runIOTest(new ReadOperation(options), readDir, sc, execMetrics)
    execMetrics.externalExecTime += System.currentTimeMillis() - tStart
    execMetrics
  }

  def randomReadTest(fs: FileSystem, sc: SparkContext): ExecMetrics = {
    val execMetrics = new ExecMetrics(sc)

    val readDir = SparkConfigHelper.getRandomReadDir(sc.getConf)
    fs.delete(readDir, true)
    val tStart = System.currentTimeMillis()
    runIOTest(new RandomReadOperation(options), readDir, sc, execMetrics)
    execMetrics.externalExecTime += System.currentTimeMillis() - tStart
    execMetrics
  }

  /**
   * *
   * Make spark confguriation and add any additional properties passed in
   */
  def mkSparkConfiguration(options:Options=options): SparkConf = {
    
    val conf = new SparkConf().setAppName("SparkDFSIO")

    // add additional properties
    if (!additionalSparkProperties.isEmpty) {
      additionalSparkProperties.entrySet.asScala.foreach(mapEntry ⇒ conf.set(mapEntry.getKey.toString, mapEntry.getValue.toString))
    }

    // disable speculative execution
    conf.set("spark.speculation", "false")
    conf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    
       LOG.info("baseDir = " + SparkConfigHelper.getBaseDir(sparkConf));

    if (options.outputCodec != null) {
      sparkConf.set("test.io.compression.class", options.outputCodec);
      LOG.info("compressionClass = " + options.outputCodec);
    }

    sparkConf.set("test.io.file.buffer.size", options.bufferSize.toString);
    sparkConf.set("test.io.skip.size", options.skipSize.toString);

    // options for schema merge
    
    sparkConf.set("spark.sql.parquet.mergeSchema", options.parquetSchemaMerge.toString)
    
    conf
  }

  def toMB(bytes: Long) = (bytes.toFloat) / MEGA

  def mkSparkContext(sparkConf:SparkConf, options:Options):SparkContext = {
     LOG.info("Starting Spark context")
    val sc = new SparkContext(sparkConf)
     
     // setup parquet block and page sizes
     sc.hadoopConfiguration.setLong("dfs.blocksize", options.parquetBlockSize)
     sc.hadoopConfiguration.setLong("parquet.block.size", options.parquetBlockSize)
     sc.hadoopConfiguration.setLong("parquet.page.size", options.parquetPageSize)
     
     // set parquet metadata settings
     sc.hadoopConfiguration.set("parquet.enable.summary-metadata", options.summaryMetadata.toString)
     
     // set parquet direct output committer
     if (options.parquetDirect)
     sc.hadoopConfiguration.set("spark.sql.parquet.output.committer.class", "org.apache.spark.sql.parquet.DirectParquetOutputCommitter")
     
     sc
  }
  
  /**
   * *
   * main processing
   */
  def main(args: Array[String]) {

    LOG.info("Starting streaming app")

    options = processOptions(args)

    validateOptions

    val resFileName = DEFAULT_RES_FILE_NAME

    sparkConf = mkSparkConfiguration(options)

    runPreStartChecks

    if (options.testType == null) {
      LOG.error("No test specified to run")
      return -1;
    }

 

   val sc = mkSparkContext(sparkConf, options)

    val fs = FileSystem.get(sc.hadoopConfiguration)

    if (options.isSequential) {
      val metrics = new ExecMetrics(sc)
      val tStart = System.currentTimeMillis()
      sequentialTest(fs, options.testType, options.fileSize, options.nrFiles, sc, metrics);
      val execTime = System.currentTimeMillis() - tStart;
      val resultLine = "Seq Test exec time sec: " + execTime.toFloat / 1000
      LOG.info(resultLine)
    } else if (options.testType == TestType.TEST_TYPE_CLEANUP) {
      cleanup(fs, sparkConf);
    } else {
      createControlFile(fs, options.fileSize, options.nrFiles, sc.getConf)
      val tStart = System.currentTimeMillis()
      val metrics = options.testType match {
        case TestType.TEST_TYPE_WRITE         ⇒ writeTest(fs, sc)
        case TestType.TEST_TYPE_READ          ⇒ readTest(fs, sc)

        case TestType.TEST_TYPE_APPEND        ⇒ appendTest(fs, sc)

        case TestType.TEST_TYPE_READ_RANDOM   ⇒ randomReadTest(fs, sc)
        case TestType.TEST_TYPE_READ_BACKWARD ⇒ randomReadTest(fs, sc)
        case TestType.TEST_TYPE_READ_SKIP     ⇒ randomReadTest(fs, sc)

        case TestType.TEST_TYPE_TRUNCATE      ⇒ truncateTest(fs, sc)

        case _                                ⇒ null
      }
      val execTime = System.currentTimeMillis() - tStart

      LOG.info(s"Elapsed time(s): ${Math.floor(execTime / 1000)}") 
      analyzeResult(metrics, options.testType, resFileName)
    }

    sc.stop()

  }

  def cleanup(fs: FileSystem, conf: SparkConf): Unit = {
    LOG.info("Cleaning up test files");
    fs.delete(new Path(SparkConfigHelper.getBaseDir(conf)), true);
  }

  def analyzeResult(metrics: ExecMetrics,
                    testType: String,
                    resFileName: String): Unit = {
    val med = metrics.rate / 1000 / metrics.tasks.value
    val stdDev = Math.sqrt(Math.abs(metrics.sqrate / 1000 / metrics.tasks.value - med * med))
    val df: DecimalFormat = new DecimalFormat("#.##")
    val resultLines = Array[String](
      "----- TestSparkIO ----- : " + testType,
      "            Date & time: " + new Date(System.currentTimeMillis()),
      "        Number of files: " + metrics.tasks.value,
      "   Total Rows processed: " + metrics.totalRows.value,
      " Total MBytes processed: " + df.format(toMB(metrics.totalSize.value)),
      "      Throughput mb/sec: " + df.format(metrics.totalSize.value * 1000.0 / (metrics.ioExecTime.value * MEGA)),
      "Total Throughput mb/sec: " + df.format(toMB(metrics.totalSize.value) / (metrics.ioExecTime.value.toFloat)),
      " Average IO rate mb/sec: " + df.format(med),
      "  IO rate std deviation: " + df.format(stdDev),
      "     Test exec time sec: " + df.format(metrics.execTime.value.toFloat / 1000),
      "     Test io exec time sec: " + df.format(metrics.ioExecTime.value.toFloat / 1000),
      "Test external exec time(s): " + df.format(metrics.externalExecTime.toFloat / 1000),
      "")

    var res: PrintStream = null
    try {
      res = new PrintStream(new FileOutputStream(new File(resFileName), true));
      for (i ← 0 until resultLines.length) {
        LOG.info(resultLines(i))
        res.println(resultLines(i))
      }
    } finally {
      if (res != null) res.close()
    }
  }

  def analyzeResult(fs: FileSystem, testType: String, metrics: ExecMetrics) {
    val dir = System.getProperty("test.build.dir", "target/test-dir")
    analyzeResult(metrics, testType, dir + "/" + DEFAULT_RES_FILE_NAME);
  }

}


