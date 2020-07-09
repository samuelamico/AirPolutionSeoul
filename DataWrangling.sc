import org.apache.spark.sql.SparkSession

val spark = SparkSession
  .builder()
  .master("local")
  .appName("Spark SQL basic example")
  .config("spark.some.config.option", "some-value")
  .getOrCreate()

val itemDF = spark.read.csv("C:\\Users\\samfs\\Desktop\\Projetos-Samuel\\AirPolutionSeoul\\Python\\Resources\\Data_Measurement_item_info.csv")
itemDF.show()
//// Function to read Files:

val measurement_item_file: String = "AirPolution/Data_Measurement_item_info.csv"
val measurement_station_file: String  = "Data_Measurement_station_info.csv"
val measurement_info_file: String  = "Measurement_info.csv"
val measurement_file: String  = "Measurement_summary.csv"



