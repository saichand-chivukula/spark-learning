import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}
import java.sql.Timestamp

object OrderExplicit extends App {

  case class Orders(order_id: Int, order_date: Timestamp, customer_id: Int, order_status: String)

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "OrdersSpark")
  sparkConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  val orderSchema = StructType(List(
    StructField("orderid", IntegerType),
    StructField("orderdate", TimestampType),
    StructField("customerid", IntegerType),
    StructField("orderstatus", StringType)
  )
  )

  val ordersSchemaDDL = "orderId Int, orderDate TimeStamp, custId Int, orderStatus String"

  val ordersDF = spark.read
    .format("csv")
    .option("header", "true")
    .schema(ordersSchemaDDL)
    .option("path", "/Users/saichandchivukula/Desktop/Datasets/orders-201019-002101.csv")
    .load()

  import spark.implicits._

  val ordersDS = ordersDF.as[Orders]

  ordersDF.printSchema()

  ordersDF.show()

  scala.io.StdIn.readLine()

}