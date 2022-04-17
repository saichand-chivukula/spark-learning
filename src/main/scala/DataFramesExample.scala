import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DataFramesExample extends App{
  //Spark Session is a singleton which means we can create only one spark session for one application
  //Below is the other way for doing the Spark Session initalization
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "DataFrameExample")
  sparkConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .appName("DataFramesExample")
    .master("local[2]")
    .getOrCreate()

  // Spark session is a singleton object

  spark.stop()

}
