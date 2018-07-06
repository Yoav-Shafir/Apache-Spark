package dev.rdd.WordCount

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark._

object WordCount {

  def main(args: Array[String]) {
  
    // Set logging to show only errors.
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Run application locally on a 3 worker threads.
    val conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]")
    val sc = new SparkContext(conf)
    
    // read a text file which gives us RDD of String.
    val lines = sc.textFile("data/in/word_count.text")
    
    // `flatMap` applies a function that returns a sequence for each element in the list,
    // then, flatten the result.
    val words = lines.flatMap(line => line.split(" "))
    
    // Return the count of each unique value in this RDD as a local map of (value, count) pairs.
    val wordCounts = words.countByValue()
    
    // print map.
    for ((word, count) <- wordCounts) println(word + " : " + count)
  }
}