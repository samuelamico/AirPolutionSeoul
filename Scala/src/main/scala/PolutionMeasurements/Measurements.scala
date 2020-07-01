package PolutionMeasurements

import org.apache.spark.{SparkConf, SparkContext}

case class Measurements(date: String, stationCode: Int, itemCode: Int, avgValue: Double, instrumentStatus: Int){
  override def toString: String = s"Date: $date, StationCode: $stationCode, ItemCode: $itemCode, AverageValue: $avgValue"
}

case class Item(itemCode: Int, itemName: String, unitMeasurement: String, blueStatus: Float, greenStatus: Float, yellowStatus: Float, redStatus: Float){
  override def toString: String = s" Code: $itemCode , Name: $itemName, Unit of Measurement: $unitMeasurement"
}

case class Station(stationCode: Int, stationName: String, address: String, latitude: Double, longitude: Double){
  override def toString: String = s" Code: $stationCode, Name: $stationName, Address: $address, Latitude: $latitude, Longitude: $longitude "
}


object MeasurementsSummary extends MeasurementesInterface{

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("PolutionSeaoul")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

}

