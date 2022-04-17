import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}

object OrdersSpark extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "OrdersSpark")
  sparkConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  val ordersDF = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "/Users/saichandchivukula/Desktop/Datasets/orders-201019-002101.csv")
    .load()

  ordersDF.printSchema()
  ordersDF.show()

  scala.io.StdIn.readLine()
  spark.stop()

}
