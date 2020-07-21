package PolutionMeasurements

import org.apache.spark.sql.types._

import scala.io._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object StaticInformation extends MeasurementesInterface {

  // This Object refers to the StationReading and wrangling
  // Spark Session
  val spark = SparkSession.builder()
    .appName("Polution on Seoul")
    .master("local[*]")
    .config("spark.ui.port", "8081")
    .getOrCreate()


  /**
   *
   * @param stationfile The name of the file that is in the resource
   * @return a Dataframe correspond to the station file read
   */
  def readFile(stationfile: String): Dataset[Row] = {
    val path = Source.getClass.getResource(stationfile).getPath

    //Struct Schema:
    val schema = StructType(List(
      StructField("station_code",IntegerType,nullable = false),
      StructField("station_name",StringType,nullable = true),
      StructField("address",StringType,nullable = true),
      StructField("latitude",DoubleType,nullable = true),
      StructField("longitude",DoubleType,nullable = true)
    ))

    val stationDF = spark.read.schema(schema)
      .format("csv")
      .load(path)
      .na.drop(Seq("latitude","longitude","station_code"))

    stationDF
  }





}
