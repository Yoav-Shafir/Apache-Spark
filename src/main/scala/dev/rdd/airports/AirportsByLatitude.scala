package dev.rdd.airports.AirportsByLatitude

import dev.utils.Utilities._
import org.apache.spark.{SparkConf, SparkContext}

 /* 
   TASK:
    
   Create a Spark program to read the airport data from in/airports.text,  find all the airports whose latitude are bigger than 40.
   Then output the airport's name and the airport's latitude to out/airports_by_latitude.text.
   Each row of the input file contains the following columns:
   Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
   ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format
   Sample output:
   "St Anthony", 51.391944
   "Tofino", 49.082222
   ...
 */

object scala {
  
  def main(args: Array[String]) {
    
    val conf = new SparkConf().setAppName("airports").setMaster("local[2]")
    val sc = new SparkContext(conf)
    
    setupLogging()
    
    val airports = sc.textFile("data/in/airports.text")
    val airportsInUSA = airports.filter(line => line.split(COMMA_DELIMITER)(6).toFloat > 40)

    val airportsNameAndCityNames = airportsInUSA.map(line => {
      val splits = line.split(COMMA_DELIMITER)
      splits(1) + ", " + splits(6)
    })

    airportsNameAndCityNames.saveAsTextFile("data/out/airports_by_latitude.text")
  }
}