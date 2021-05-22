package get_taxi.models

import java.time.{LocalDate}
import java.util.Date

case class Trip(driverID: Int, city: String, distance: Int, tripDate: LocalDate) {

}
