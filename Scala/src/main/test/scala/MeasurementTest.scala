import PolutionMeasurements._
import org.apache.log4j.{Level, Logger}
import org.scalatest.{FlatSpec, Matchers}

class MeasurementTest extends FlatSpec with Matchers{
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  val station = StaticInformation.readStation("/AirPolution/Data_Measurement_station_info.csv")
  val measureDF = StaticInformation.readMeasurement("/AirPolution/Measurement_summary.csv")

  it should "return the count" in {
    station.count() shouldEqual(25)
  }

  it should "return the total of measurements" in {
    measureDF.count() shouldEqual(647511)
  }


  it should "return the timezone in dd/MM/yyyy hh:mm:ss" in {
    StaticInformation.convertTimezone("2017-01-01 00:00") shouldEqual("2017/01/01 00:00:00")
  }

}
