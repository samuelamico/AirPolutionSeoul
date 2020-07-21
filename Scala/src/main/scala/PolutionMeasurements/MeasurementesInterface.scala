package PolutionMeasurements

import org.apache.spark.sql.{Dataset, Row}

trait MeasurementesInterface {
  def stationFile(stationfile: String): Dataset[Row]
  def itemFile(itemfile: String): Dataset[Row]
}
