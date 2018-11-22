/**
 * This module contains a solution for Popular POIs Challenge.
 * The input are files with Uber rides data and POIs data
 * The output is a files with POI names sorted down by popularity (number of visits), i.e. most visited POIs are in top.
 */

import spark._
import org.apache.spark.SparkContext._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import scala.util.Try

/**
 * Class for a geographical coordinate
 * Each coordinate has Latitude and Longitude
 */
case class Coordinate(val lat: Double, val lon: Double) {
  /**
   * The distance between two coordinates, in meters.
   */
  def distanceTo(other: Coordinate): Double = {
    val lat1 = math.Pi / 180.0 * lat
    val lon1 = math.Pi / 180.0 * lon
    val lat2 = math.Pi / 180.0 * other.lat
    val lon2 = math.Pi / 180.0 * other.lon
    // Uses the haversine formula:
    val dlon = lon2 - lon1
    val dlat = lat2 - lat1
    val a = math.pow(math.sin(dlat / 2), 2) + math.cos(lat1) * math.cos(lat2) * math.pow(math.sin(dlon / 2), 2)
    val c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    val meters = 6367 * c
    meters
  }
}

/**
 * Class for Uber rides
 * Each Uber ride has its timestamp, location of pick up/drop off point, base 
 */
case class UberRide(
  val datetime: org.joda.time.DateTime,
  val location: Coordinate,
  val base: String
)

/**
 * Class for points of interest (POI)
 * Each POI has a location, place type, name
 */
case class POI(
  val location: Coordinate,
  val place_type: String,
  val name: String
)

/**
 * Auxiliary functions
 */
object PopularPOIsUtils {
  /**
   * This function converts a String variable to a DateTime format
   */
  def getLocalDateTime(timestamp: String): org.joda.time.DateTime = {
    val pattern1 = DateTimeFormat.forPattern("\"M/d/yyyy H:mm:ss\"")
    val pattern2 = DateTimeFormat.forPattern("\"M/dd/yyyy H:mm:ss\"")
    val pattern3 = DateTimeFormat.forPattern("\"MM/d/yyyy H:mm:ss\"")
    val pattern4 = DateTimeFormat.forPattern("\"MM/dd/yyyy H:mm:ss\"")
    val pattern5 = DateTimeFormat.forPattern("\"M/d/yyyy HH:mm:ss\"")
    val pattern6 = DateTimeFormat.forPattern("\"M/dd/yyyy HH:mm:ss\"")
    val pattern7 = DateTimeFormat.forPattern("\"MM/d/yyyy HH:mm:ss\"")
    val pattern8 = DateTimeFormat.forPattern("\"MM/dd/yyyy HH:mm:ss\"")
    
    val result = Try {
        pattern1.parseDateTime(timestamp)
      } recover {
        case _ => pattern2.parseDateTime(timestamp)
      } recover {
        case _ => pattern3.parseDateTime(timestamp)
      } recover {
        case _ => pattern4.parseDateTime(timestamp)
      } recover {
        case _ => pattern5.parseDateTime(timestamp)
      } recover {
        case _ => pattern6.parseDateTime(timestamp)
      } recover {
        case _ => pattern7.parseDateTime(timestamp)
      } recover {
        case _ => pattern8.parseDateTime(timestamp)
      }
    result.get
  }
  
  /**
   * This function retrieves a string (that comes from an input file) and converts it to an object of UberRide class
   */
  def stringToUberRide(s: String): UberRide = {
      val splits = s.split(",")
      val datetime = getLocalDateTime(splits(0))
      val lat = splits(1).toDouble
      val lon = splits(2).toDouble
      val base = splits(3).replace("\"", "")
      new UberRide(datetime, new Coordinate(lat, lon), base)
  }

  /**
   * This function retrieves a string (that comes from an input file) and converts it to an object of POI class
   */
  def stringToPOI(s: String): POI = {
      val splits = s.split(",")
      val lat = splits(0).toDouble
      val lon = splits(1).toDouble
      val place_type = splits(2)
      val name = splits(3)
      new POI(new Coordinate(lat, lon), place_type, name)
  }
} 

//--------------------------------------------------------------
import PopularPOIsUtils._

/**
 * Reading the input file with Uber rides data and converting it to a RDD of UberRide objects
 */
val fnameUberRides = "/home/ec2-user/HERE_Popular_POI_Challenge/input_data_sets/uber-raw-data-apr14.csv"
val numSplits = 1
val uberRidesRawData = sc.textFile(fnameUberRides, numSplits)

// removing the header
val uberRidesRawDataWithoutHeader = uberRidesRawData.mapPartitionsWithIndex {
  (idx, iter) => if (idx == 0) iter.drop(1) else iter 
}
val uberRides = uberRidesRawDataWithoutHeader.map(s => stringToUberRide(s))

//--------------------------------------------------------------
/**
 * Reading the input file with points of interest (POI) data and converting it to a RDD of POI objects
 */
val fnamePOI = "/home/ec2-user/HERE_Popular_POI_Challenge/input_data_sets/poi.csv"
val poiRawData = sc.textFile(fnamePOI, numSplits)

// removing the header
val poiRawDataWithoutHeader = poiRawData.mapPartitionsWithIndex {
  (idx, iter) => if (idx == 0) iter.drop(1) else iter 
}
val pois = poiRawDataWithoutHeader.map(s => stringToPOI(s))

//-------------------------------
// Constants for morning and evening boundary times
val MorningStartTime ="07:00:00"
val MorningEndTime ="10:00:00"
val EveningStartTime ="18:00:00"
val EveningEndTime ="21:00:00"

// uberRidesMorningAndEvening is a RDD that contains only relevant Uber rides for morning and evening hours
val uberRidesMorningAndEvening = uberRides.map{ur => (ur, ur.datetime.toString().slice(11,19))}.filter{case(ur, urtime) => ((urtime >= MorningStartTime && urtime < MorningEndTime) || (urtime >= EveningStartTime && urtime < EveningEndTime))}.map{case(ur,urtime) => ur}

// allPOIsAndUberRides is a RDD that contains all possible combinations of all points of interest and all relevant Uber rides
val allPOIsAndUberRides = pois.cartesian(uberRidesMorningAndEvening)

// Constant for number of meters when a point of interest considered as visited
val CheckInDistance = 100

// visitedPOIs is a RDD that contains names of visited points of interests per Uber ride
val visitedPOIs = allPOIsAndUberRides.map{case(poi, uberRide) => {
  (poi.name, poi.location.distanceTo(uberRide.location))
}}.filter{case(poi_name, distance) => distance <= CheckInDistance}.map{case(poiName, distance) => poiName}

// poisPopularity is a RDD that contains POI name and number of its visits
val poisPopularity = visitedPOIs.groupBy(identity).mapValues(_.size)

// mostPopularPOIsNames is a RDD that contains POI names sorted down by popularity (most visited POIs are in the top)
val mostPopularPOIsNames = sc.parallelize(r.sortBy(_._2).collect().reverse.map{case(poi_name, n) => poi_name})

// Path for the output directory
val pathOutputFolder = "/home/ec2-user/HERE_Popular_POI_Challenge/output/"

// Saving the sorted list of most popular POIs in the file
mostPopularPOIsNames.saveAsTextFile(pathOutputFolder)
