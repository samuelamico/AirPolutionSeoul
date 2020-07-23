package PolutionMeasurements

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object StaticInformation extends MeasurementesInterface {

  // This Object refers to the StationReading and wrangling
  // Spark Session
  val spark = SparkSession.builder()
    .appName("Polution on Seoul")
    .master("local[*]")
    .config("spark.ui.port", "8081")
    .getOrCreate()

  val path = (file:String) => getClass.getResource(file).getPath

  /**
   *
   * @param stationfile The name of the file that is in the resource
   * @return a Dataframe correspond to the station file read
   */
  def stationFile(stationfile: String): Dataset[Row] = {

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
    def itemFile(itemfile: String): Dataset[Row] = {

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
  def readMeasurement(measuredfile: String) = {
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
      .load(path(measuredfile))

    measureDF
  }



}
