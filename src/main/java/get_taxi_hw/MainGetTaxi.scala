package get_taxi_hw

import get_taxi_hw.get_taxi.models.{Driver, Trip}
import get_taxi_hw.get_taxi.utils.GetTaxi
import get_taxi_hw.get_taxi.utils.Helpers.RddStrings
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.time.LocalDate

object MainGetTaxi {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    System.setProperty("hadoop.home.dir", "C:\\tmp\\winutils")

    val sparkConf = new SparkConf().setAppName("Hello spark from scala").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val driversRDD: RDD[String] = sc.textFile("data/taxi/drivers.txt")
    val tripsRDD: RDD[String] = sc.textFile("data/taxi/trips.txt")

    val drivers: RDD[Driver] = driversRDD.map(driverRec => driverRec.getDriver(","))
    val trips: RDD[Trip] = tripsRDD.map(tripRec => tripRec.getTrip(" "))

    val countTripsByCityAndDistance = GetTaxi.countTripsByCityAndDistance(trips, "boston", "=", 10, ">")
    val sumDistanceByCity = GetTaxi.sumDistanceByCity(trips, "boston", "=")
    val topNDrivers = GetTaxi.getTopNDriversByOrderedByLongestDistance(drivers, trips, 3, LocalDate parse "2016-02-17", false)

    GetTaxi.displayDriversList(drivers, GetTaxi.countDrivers(drivers))
    GetTaxi.displayTripsList(trips, GetTaxi.countTrips(trips))
    GetTaxi.displayCountTripsByCityAndDistance(countTripsByCityAndDistance)
    GetTaxi.displaySumDistanceByCity(sumDistanceByCity)
    GetTaxi.displayTopNDrivers(topNDrivers)
  }
}
