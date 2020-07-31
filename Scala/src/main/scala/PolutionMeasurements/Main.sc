package PolutionMeasurements

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}
case class Item(timezone: String, SO2: Double, NO2: Double, O3: Double,CO: Double,PM10: Double)

case class levelRisk(good: Any, normal: Any, bad: Any, veryBad: Any)

object Main extends App{

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  val spark = SparkSession
    .builder()
    .master("local")
    .appName("Spark SQL basic example")
    .config("spark.ui.port", "8081")
    .getOrCreate()



  val eventDF = StaticInformation.descentrilizer()
  val item = StaticInformation.readItem("/AirPolution/Data_Measurement_item_info.csv").collect().toList.tail
  val itemMap = item.map(row => ( row(1) -> levelRisk(row(3),row(4),row(5),row(6)) ) ).toMap


  //println(s"Item List = $item")
  //println(s"Item Map = $itemMap")
  println(itemMap("SO2").good)



  val encoder = org.apache.spark.sql.Encoders.product[Item]

  ///////// ---------------- Adding column:
  def riskSo2(value: Double) = value match {
    case n if(n < 0.02) => "Good"
    case n if(n < 0.05) => "Normal"
    case n if(n < 0.15) => "Bad"
    case n if(n < 1.0) => "Very Bad"

  }

  val udfSO2 = udf(riskSo2 _)

  val eventElement = eventDF.select(
    col("timezone"),
    col("SO2"),
    col("NO2"),
    col("O3"),
    col("CO"),
    col("PM10")
  ).withColumn("Level_SO2",udfSO2(col("SO2")))


  //eventElement.show()




}
