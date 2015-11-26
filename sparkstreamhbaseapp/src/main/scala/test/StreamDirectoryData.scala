package test

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.storage.StorageLevel
import java.io._
import org.apache.hadoop.io.{ LongWritable, Text }
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.{ SparkContext, SparkConf }
//import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.{ Seconds, StreamingContext }

object StreamDirectoryData {

  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: StreamDirectoryData <duration> <dataDirectory> <outputDirectory>")
      System.exit(1)
    }

    val duration = args(0);
    val inputDirectory = args(1)
    val outputDirectory = args(2)

    // Create the context with a 1 second batch size
    val sparkConf = new SparkConf().setAppName("StreamDirectoryData")
    val ssc = new StreamingContext(sparkConf, Seconds(duration.toLong))

    //Read data from directory in stream
    //val directoryData = ssc.textFileStream(inputDirectory)
    //val lines = ssc.fileStream[LongWritable, Text, TextInputFormat](inputDirectory).map { case (x, y) => (x.toString, y.toString) }
    //lines.print()
    
    //Read data
    val dstream2 = ssc.textFileStream(inputDirectory)
    dstream2.print()
    
    //Store in HDFS
    dstream2.foreachRDD(rdd => rdd.saveAsTextFile(outputDirectory + "/output_" ))
    
   // val mapData = dstream2.foreachRDD(rdd => rdd.map { x => x.split(",") } )
   // println(mapData)
    
    print("Done saving..")
    invokeAggregation(outputDirectory)
    

    ssc.start()
    ssc.awaitTermination()
    
    //ssc.close();
  }

  def invokeAggregation(outputDirectory : String){
    println("Inside invoke aggregation...")
  //Read the part files and start aggregation
    val fileList = getListOfFiles(outputDirectory + "/output_")
    println(fileList)
    println(fileList.size)
    println("Done aggregation..")
  } 
  
  def getRecursiveListOfFiles(dir: File): Array[File] = {
    val these = dir.listFiles
    these ++ these.filter(_.isDirectory).flatMap(getRecursiveListOfFiles)
  }

  def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }
}