import org.apache.spark.sql.SparkSession


object Main extends App {

  val spark = SparkSession
    .builder()
    .master("local")
    .appName("Spark SQL basic example")
    .config("spark.ui.port", "8081")
    .getOrCreate()

  // Reading Path from resources:
  def getPath(name: String)  = {
    getClass.getResource(name).getPath
  }
  val path = getPath("AirPolution/Data_Measurement_item_info.csv")

  val itemDF = spark.read.format("csv").option("header", "true").load(path)

  //itemDF.show()

  //itemDF.printSchema()

  // Measuremente information:
  val pathMeasuInfo = getPath("AirPolution/Measurement_info.csv")
  val measureInfoDF = spark.read.format("csv").option("header","true").load(pathMeasuInfo)

  //measureInfoDF.show()

  // Filter only for item code = 1, on date 210-01-01:
  import spark.implicits._
  import org.apache.spark.sql.functions._

  def splitDate = udf(( date : String) => {
      date.split(" ")(1)
  })

  val measuremtSO2 = measureInfoDF.select($"Average value",$"Measurement Date")
    .filter($"Item code" === 1)
    .withColumn("Time",splitDate($"Measurement date"))
    .groupBy($"Time")
    .agg(sum($"Average value").alias("Sum of Averages"))

  measureInfoDF.select($"Average value",$"Measurement date",$"Item code")
    .groupBy($"Item code")
    .agg(sum($"Average value").alias("Sum"))

  ///////////////////

  


}
