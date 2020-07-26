package PolutionMeasurements

import org.apache.spark.sql.{Dataset, Row}

trait MeasurementesInterface {
  def readStation(stationfile: String): Dataset[Row]
  def readItem(itemfile: String): Dataset[Row]
  def readMeasurement(measuredfile: String): Dataset[Row]
  def joinedDF(stationDF: Dataset[Row],itemDF: Dataset[Row],measureDF: Dataset[Row]): Dataset[Row]
  def convertTimezone(timezone: String): String
  def riskLevel(eventDF: Dataset[Row],itemDF: Dataset[Row])
}
