package PolutionMeasurements

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row, SparkSession}


case class Item(timezone: String, SO2: Double, NO2: Double, O3: Double,CO: Double,PM10: Double,PM25: Double)

object StaticInformation extends MeasurementesInterface {

  // Pure Functions
  def convertTimezone(timezone: String): String = {
    val timezoneList: List[String] = timezone.split(" ").toList
    val date = timezoneList(0)
      .split("-")
      .fold("")(_+"/"+_)
      .substring(1)
    val time = timezoneList(1) + ":00"

    date + " " + time

  }

  def setLevel(value: Double) = {


  }


  // This Object refers to the StationReading and wrangling
  // Spark Session
  val spark = SparkSession.builder()
    .appName("Polution on Seoul")
    .master("local[*]")
    .config("spark.ui.port", "8081")
    .getOrCreate()

  val path = (file:String) => getClass.getResource(file).getPath
  val udfTime = udf(convertTimezone _)
  /**
   *
   * @param stationfile The name of the file that is in the resource
   * @return a Dataframe correspond to the station file read
   */
  def readStation(stationfile: String): Dataset[Row] = {

    //Struct Schema:
    val schema = StructType(List(
      StructField("station_code", IntegerType, nullable = true),
      StructField("station_name", StringType, nullable = true),
      StructField("address", StringType, nullable = true),
      StructField("latitude", DoubleType, nullable = true),
      StructField("longitude", DoubleType, nullable = true)
    ))

    val stationDF = spark.read.schema(schema)
      .format("csv")
      .option("header","true")
      .load(path(stationfile))
      .na.drop(Seq("latitude", "longitude", "station_code"))

    stationDF
  }
    /**
     *
     * @param itemfile Path for Item of Measurements
     * @return the Itens Datframe
     */
    def readItem(itemfile: String): Dataset[Row] = {

      // schema
      val schema = StructType(List(
        StructField("item_code",IntegerType,nullable = false),
        StructField("item_name",StringType,nullable = true),
        StructField("unit",StringType,nullable = true),
        StructField("good",DoubleType,nullable = false),
        StructField("normal",DoubleType,nullable = false),
        StructField("bad",DoubleType,nullable = false),
        StructField("very_bad",DoubleType,nullable = false)
      ))

      val itemDF = spark.read.schema(schema)
        .format("csv")
        .load(path(itemfile))

      itemDF

    }

  /**
   *
   * @param measuredfile, Path to the Measurement data
   * @return the measure DataFrame
   */
  def readMeasurement(measuredfile: String): Dataset[Row] = {
    // schema
    val schema = StructType(List(
      StructField("timezone",StringType,nullable = false),
      StructField("station_code",IntegerType,nullable = true),
      StructField("address", StringType, nullable = true),
      StructField("latitude", DoubleType, nullable = true),
      StructField("longitude", DoubleType, nullable = true),
      StructField("SO2",DoubleType,nullable = true),
      StructField("NO2",DoubleType,nullable = true),
      StructField("O3",DoubleType,nullable = true),
      StructField("CO",DoubleType,nullable = true),
      StructField("PM10",DoubleType,nullable = true),
      StructField("PM2.5",DoubleType,nullable = true)
    ))

    val measureDF = spark.read.schema(schema)
      .format("csv")
      .option("header",true)
      .load(path(measuredfile))
      .withColumn("timezone",udfTime(col("timezone")))

    measureDF
  }


  /// Now the CoreFunctions:
  def joinedDF(stationDF: Dataset[Row],itemDF: Dataset[Row],measureDF: Dataset[Row]): Dataset[Row] ={

    val interStation = stationDF.select(col("station_code"),col("station_name"))
    measureDF.join(interStation,Seq("station_code"),"left")


  }

  def descentrilizer() ={

    // Reading Files:
    val stationDF = readStation("/AirPolution/Data_Measurement_station_info.csv")
    val itemDF = readItem("/AirPolution/Data_Measurement_item_info.csv")
    val measureDF = readMeasurement("/AirPolution/Measurement_summary.csv")

    // Join Files
    joinedDF(stationDF,itemDF,measureDF)

  }

  def riskLevel(eventDF: Dataset[Row],itemDF: Dataset[Row]) = {
    val eventDS = eventDF.select(
      col("timezone"),
      col("SO2"),
      col("NO2"),
      col("O3"),
      col("CO"),
      col("PM10"),
      col("PM2.5")
    )
      .as[Item]
      .toDS

    eventDS.map(measure => 
      func(
        measure.SO2
        )
      )


  }


}
