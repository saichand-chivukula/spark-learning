import jdk.jfr.Timestamp
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}

case class OrdersData(order_id: Int, order_date:Timestamp, order_customer_id: Int, order_status: String)

object WordCountDF  extends App {

  //Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "WordCountDF")
  sparkConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  //read is a action(no of actions is equal to no of jobs)
  val ordersDF = spark.read
    .option("header", true)
    .option("inferSchema", true) // this is also an action, mentioning like this is not a good practice as in prod we will be having mixed data
    .csv("/Users/saichandchivukula/Desktop/Datasets/orders-201019-002101.csv")

  import spark.implicits._ // this is required in order to convert from dataframe to dataset and dataset to data frame
  val ordersDS = ordersDF.as[OrdersData]

  //ordersDS.filter(x => x.order_id < 10)
  //ordersDF.filter("order_ids < 10").show

  // Which are more prefered when compared to Datasets and Dataframes
  //Dataframes are compared to Datasets

  val groupedOrders = ordersDF
    .repartition(4)
    .where("order_customer_id > 10000")
    .select("order_id", "order_customer_id")
    .groupBy("order_customer_id")
    .count()
  // All the above are transformations, there is no action in the above statement

    groupedOrders.foreach(x =>{
      println(x)
    })
  // this is a third action
  //ordersDF.show()// this gives 20 rows as the output
  //No  of stages are dependent on shuffle boundaries
  groupedOrders.show(50)
  Logger.getLogger(getClass.getName).info("My application is completed successfully")
  //ordersDF.printSchema()

  scala.io.StdIn.readLine()
  spark.stop()

}
