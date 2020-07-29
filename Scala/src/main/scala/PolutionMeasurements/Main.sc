package PolutionMeasurements

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}
case class Item(timezone: String, SO2: Double, NO2: Double, O3: Double,CO: Double,PM10: Double) {


}

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

  print(item)



  val encoder = org.apache.spark.sql.Encoders.product[Item]

  ///////// ---------------- Adding column:
  def riskSo2(value: Double) = value match {
    case n if(n < 0.02) => "Good"
  }
  
  val udfSO2 = udf(riskSo2 _)
  
  val eventElement = eventDF.select(
    col("timezone"),
    col("SO2"),
    col("NO2"),
    col("O3"),
    col("CO"),
    col("PM10")
  )
      .withColumn("Level_SO2",)


  eventDS.show()

  item(1)




  //val result = StaticInformation.riskLevel(eventDF,itemDF)
  //result.show()


}
