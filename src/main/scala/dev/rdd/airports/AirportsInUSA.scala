package dev.rdd.airports

import dev.utils.Utilities._
import org.apache.spark.{SparkConf, SparkContext}


/* TASK:

 Create a Spark program to read the airport data from in/airports.text, find all the airports which are located in United States
   and output the airport's name and the city's name to out/airports_in_usa.text.
   Each row of the input file contains the following columns:
   Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
   ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format
   Sample output:
   "Putnam County Airport", "Greencastle"
   "Dowagiac Municipal Airport", "Dowagiac"
   ...
 */

object AirportsInUSA {
  
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("airports").setMaster("local[2]")
    val sc = new SparkContext(conf)
    
    setupLogging()
    
    val airports = sc.textFile("data/in/airports.text")
    val airportsInUSA = airports.filter(line => line.split(COMMA_DELIMITER)(3) == "\"United States\"")
    
    val airportsNameAndCityNames = airportsInUSA.map(line => {
      val splits = line.split(COMMA_DELIMITER)
      splits(1) + ", " + splits(2)
    })
    
    // !IMPORTANT make sure to delete old output file.
    airportsNameAndCityNames.saveAsTextFile("data/out/airports_in_usa.text") 
  }
}