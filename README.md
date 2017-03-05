# TestSparkIO
This project a set of benchmarks for assessing the performance of Spark reads and writes for parquet in conjunction 
with various Spark and Parquet options. It is styled after the standard HDFS IO test TestDFSIO and follows much of 
the same approach in implementation.

The test operates in two modes - read and write:

In write mode, it generates a set of parquet files containing a set of test records. The number of records to write is 
determined approximately from the '--fileSize' parameter (the size is divided by the TestData record size - which uses 
padding to ensure all records are of the same binary size).

The number of files to write is determined from the '--nrFiles' parameter.

The write operation generates a set of test data, optionally sorts it, and writes it to parquet. As number of bytes written 
is an estimate , not actually measured, the throughput numbers should be treated as a estimate of size of data read 
or to be saved. They do not account for compression, overhead in persisting, caching or spilling data but as they 
are computed the same way run to run - they serve as a good comparison metric for Parquet performance when using the same 
compression codec and storage level.

The test allows options for storage level, whether to repartition or coalesce before writing, compression codec, options
for parquet block size and page size, and various options around the merging and management of Parquet metadata

The read test will read the same data back. As the number of partitions is determined by the files on disk, the impact of spark 
settings is much less.

The tests also allow experimenting with the impact of number of executors and task, task memory 




## Open issues
* Need to implement truncate, append and random read tests
* additional unit tests needed

## arguments

Note: the files will be written to a folder on HDFS named /benchmarks

Usage: <appname>
               [genericOptions]
               -read 
               --write 
               --persist [storageLevel]   
               [--compute-before-write]
               [--compression codecClassName]
               [--nrFiles N] 
               [-size Size[B|KB|MB|GB|TB]] 
               [-resFile resultFileName] [-bufferSize Bytes]
               [--parquet] [--parquetBlockSize N] [ --parquetPageSize N] [ --parquetDictionaryPageSize N]
               [--parquetDirect]
               [--parquetDisableSummaryMetadata][--parquetEnableSummaryMetadata]
               [--parquetDisableSchemaMerge][--parquetEnableSchemaMerge]
               [--coalesce N]

## example command lines

Here are some example command lines

spark-submit ...

