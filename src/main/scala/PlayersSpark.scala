import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}

object PlayersSpark extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "OrdersSpark")
  sparkConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  val playersDF = spark.read
    .format("json")
    .option("path", "/Users/saichandchivukula/Desktop/Datasets/players-201019-002101.json")
    .option("mode", "DROPMALFORMED")
    .load()

  playersDF.show(false)

  scala.io.StdIn.readLine()

  spark.stop()

}