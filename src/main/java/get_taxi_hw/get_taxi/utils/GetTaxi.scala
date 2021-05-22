package get_taxi_hw.get_taxi.utils

import get_taxi_hw.get_taxi.models.{Driver, Trip}
import get_taxi_hw.get_taxi.utils.Helpers.RddTrip
import org.apache.spark.rdd.RDD

import java.time.LocalDate

object GetTaxi {

  def countDrivers(drivers: RDD[Driver]) = drivers.count()
  def countTrips(trips: RDD[Trip]) = trips.count()

  def countTripsByCityAndDistance(trips: RDD[Trip], city:String = "", cityFilter:String = "", distance: Int = 0, distanceFilter:String = ""): Long ={
    trips.filter(trip => trip.filterTripByCityAndDistance(city,cityFilter, distance, distanceFilter)).count()
  }

  def sumDistanceByCity(trips: RDD[Trip], city:String = "", cityFilter:String = ""): Double = {
    trips.filter(trip => trip.filterTripByCityAndDistance(city,cityFilter)).map(trip => trip.distance).sum()
  }

  def getTopNDriversByOrderedByLongestDistance(drivers:RDD[Driver], trips:RDD[Trip], numOfDrivers: Int, dateFilter: LocalDate, sortByAsc: Boolean = true): Array[(Int,(String, Int))] ={
    val driverNames: RDD[(Int, String)] = drivers.map(driver => (driver.driverID, driver.driverName))
    val driverTrips: RDD[(Int, Int)] = trips.filter(trip => trip.filterTripByCityAndDistance(tripDate = dateFilter))
                                            .groupBy(trip => trip.driverID)
                                            .mapValues(driverTrips => driverTrips.map(trip => trip.distance).sum)
    driverNames.join(driverTrips).sortBy(driver => driver._2._2,sortByAsc).take(numOfDrivers)
  }

  def displayDriversList(drivers: RDD[Driver], countDrivers: Long): Unit ={
    println("\nDrivers List: " + countDrivers)
    drivers.sortBy(driver => driver.driverID, true)
      .foreach(driver => println("Driver " + driver.driverID + ": " + driver.driverName + ", " +
        driver.address + ", " + driver.email))
  }

  def displayTripsList(trips: RDD[Trip], countTrips: Long): Unit = {
    println("\nTrips List: " + countTrips)
    trips.sortBy(trip => trip.driverID,true)
      .foreach(trip => println("Trip: " + trip.driverID + ", " + trip.city + ", " + trip.distance + ", " + trip.tripDate))
  }

  def displayCountTripsByCityAndDistance(count: Long): Unit ={
    println("\nNumber of trips by city and distance: " + count)
  }

  def displaySumDistanceByCity(sum: Double): Unit ={
    println("\nTotal distance of trips by city: " + sum)
  }

  def displayTopNDrivers(topNDrivers: Array[(Int, (String, Int))]): Unit ={
    println("\nTop Drivers List ")
    topNDrivers.foreach(driver => println(driver._2._1 + " (" + driver._1 +"): " + driver._2._2 + " Km"))
  }
}
