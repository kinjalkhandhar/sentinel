package test

import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.storage.StorageLevel
import java.io._
import org.apache.hadoop.io.{ LongWritable, Text }
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.{ SparkContext, SparkConf }
//import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.{ Seconds, StreamingContext }

object Aggregation {
  
   val sparkConf = new SparkConf().setAppName("Aggregation")
   val sparkContext = new SparkContext()
   
  def main(args: Array[String]) {
    
    if(args.length < 1){
      println("USAGE : <outputDirectory>")
      System.exit(0)
    }
   
    val outputDirectory = args(0)
    
    //Read Files from Output Directory
    val rdd1 = readFilesFromDirectory(outputDirectory) 
   
    if(rdd1.length > 0)
      aggregateData(rdd1)
    else
      System.exit(0)
    
  }
  
  def readFilesFromDirectory(outputDirectory:String) :  Array[(String, String)] ={
    val readFiles = sparkContext.wholeTextFiles(outputDirectory, 100)
    println(readFiles.collect())
    val rdd1 = readFiles.collect()
    //println(rdd1.apply(0))
    println(rdd1.length)
    rdd1
  }
 
  def aggregateData(rdd1:Array[(String,String)]) : Boolean={
     println(rdd1.grouped(2))
     for(i <- 0 until rdd1.length){
       // println("i is: " + i);
       // println("i'th element is: " + rdd1(i));
        val data = rdd1(i);
        val fileName = data._1 //FileName
        val content = data._2 //Content
        //println(content)
        
        startAggregation(content)
     }
     true
  }
  
  def startAggregation(content:String) {
    
//    val splittedData = content.split(",").map(_ split ",")//.collect { case Array(k, v) => (k, v) }.toMap
//    println(splittedData.size)
//     println(splittedData(0))
//     for(i<- 0 until splittedData.length){
//      //println(splittedData(i))
//      val fileContent = splittedData(i)
//      fileContent.groupBy { x => ??? }
//      println(fileContent(0))
//     }
    
    val splittedData = content.split(",")//.collect { case Array(k, v) => (k, v) }.toMap
    println(splittedData.size)
    val value = splittedData.groupBy { x => ??? }
//    val value = splittedData.groupBy { x => x.contains("/") }
//     println(value.values)
//     val test = value.values
//     for(i<-0 until test.size)
       
    
  }
  
}