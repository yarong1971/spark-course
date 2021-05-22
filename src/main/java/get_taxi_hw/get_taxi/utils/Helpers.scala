package get_taxi_hw.get_taxi.utils

import get_taxi_hw.get_taxi.models.{Driver, Trip}
import java.util
import java.util.Locale
import java.time.LocalDate
import java.time.format.DateTimeFormatter

object Helpers {
  implicit class RddStrings(string: String) {
    def getDriver(delimiter: String): Driver = {
      val rddString: Array[String] = util.Arrays.copyOfRange(string.split(delimiter), 0, 4)
      Driver(Integer.parseInt(rddString(0).trim), rddString(1).trim, rddString(2).trim, rddString(3))
    }

    def getTrip(delimiter: String): Trip = {
      val rddString: Array[String] = util.Arrays.copyOfRange(string.split(delimiter), 0, 9)
      val tripStringDate = rddString(3).toString() + ", " +
                           rddString(4).toString() + " " +
                           rddString(5).toString() + " " +
                           rddString(8).toString()

      //val tripStringDate = rddString.slice(3,4).mkString(" ") + rddString(9)

      val formatter = DateTimeFormatter.ofPattern("E, MMM d yyyy", Locale.US)

      Trip(Integer.parseInt(rddString(0).trim),
        rddString(1).trim,
        Integer.parseInt(rddString(2).trim),
        //LocalDateTime.now())
        LocalDate.parse(tripStringDate, formatter))
    }
  }

  implicit class RddTrip(trip: Trip) {

    def filterTripByCityAndDistance(city: String = "", cityFilter: String = "", distance: Int = 0, distanceFilter: String = "", tripDate: LocalDate = null): Boolean = {
      (cityFilter, distanceFilter, tripDate) match {
        case ("=", ">", null) => trip.city.toUpperCase() == city.toUpperCase() && trip.distance > distance
        case ("=", "<", null) => trip.city.toUpperCase() == city.toUpperCase() && trip.distance < distance
        case ("=", "=", null) => trip.city.toUpperCase() == city.toUpperCase() && trip.distance == distance
        case ("=", "!=", null) => trip.city.toUpperCase() == city.toUpperCase() && trip.distance != distance
        case ("!=", ">", null) => trip.city.toUpperCase() != city.toUpperCase() && trip.distance > distance
        case ("!=", "<", null) => trip.city.toUpperCase() != city.toUpperCase() && trip.distance < distance
        case ("!=", "=", null) => trip.city.toUpperCase() != city.toUpperCase() && trip.distance == distance
        case ("!=", "!=", null) => trip.city.toUpperCase() != city.toUpperCase() && trip.distance != distance
        case (_, ">", null) => trip.distance > distance
        case (_, "<", null) => trip.distance < distance
        case (_, "=", null) => trip.distance == distance
        case (_, "!=", null) => trip.distance != distance
        case ("=", _, null) => trip.city.toUpperCase() == city.toUpperCase()
        case ("!=", _, null) => trip.city.toUpperCase() != city.toUpperCase()
        case ("=", ">", tripDate) => trip.city.toUpperCase() == city.toUpperCase() && trip.distance > distance && trip.tripDate == tripDate
        case ("=", "<", tripDate) => trip.city.toUpperCase() == city.toUpperCase() && trip.distance < distance && trip.tripDate == tripDate
        case ("=", "=", tripDate) => trip.city.toUpperCase() == city.toUpperCase() && trip.distance == distance && trip.tripDate == tripDate
        case ("=", "!=", tripDate) => trip.city.toUpperCase() == city.toUpperCase() && trip.distance != distance && trip.tripDate == tripDate
        case ("!=", ">", tripDate) => trip.city.toUpperCase() != city.toUpperCase() && trip.distance > distance && trip.tripDate == tripDate
        case ("!=", "<", tripDate) => trip.city.toUpperCase() != city.toUpperCase() && trip.distance < distance && trip.tripDate == tripDate
        case ("!=", "=", tripDate) => trip.city.toUpperCase() != city.toUpperCase() && trip.distance == distance && trip.tripDate == tripDate
        case ("!=", "!=", tripDate) => trip.city.toUpperCase() != city.toUpperCase() && trip.distance != distance && trip.tripDate == tripDate
        case (_, ">", tripDate) => trip.distance > distance && trip.tripDate == tripDate
        case (_, "<", tripDate) => trip.distance < distance && trip.tripDate == tripDate
        case (_, "=", tripDate) => trip.distance == distance && trip.tripDate == tripDate
        case (_, "!=", tripDate) => trip.distance != distance && trip.tripDate == tripDate
        case ("=", _, tripDate) => trip.city.toUpperCase() == city.toUpperCase() && trip.tripDate == tripDate
        case ("!=", _, tripDate) => trip.city.toUpperCase() != city.toUpperCase() && trip.tripDate == tripDate
        case (_, _, tripDate) =>  trip.tripDate == tripDate
        case (_, _, _) => true
      }
    }
  }
}
