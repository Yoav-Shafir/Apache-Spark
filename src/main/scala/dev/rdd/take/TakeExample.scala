package dev.rdd.take

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object TakeExample {
  
  def main(args: Array[String]) {
    
    // Turn off logging for all the libraries under the `org` package.
    Logger.getLogger("org").setLevel(Level.OFF)
    
    // Run the application locally.
    val conf = new SparkConf().setAppName("take").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val inputWords = List("spark", "hadoop", "spark", "hive", "pig", "cassandra", "hadoop")
    
    // Distribute a local Scala collection to form an RDD.
    val wordRdd = sc.parallelize(inputWords)
    
    // Take the first 3 elements of the RDD.
    val words = wordRdd.take(3)
    
    // Print
    for (word <- words) println(word)
  }
}