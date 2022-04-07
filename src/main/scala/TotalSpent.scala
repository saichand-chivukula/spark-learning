import org.apache.spark.SparkContext
import org.apache.log4j.{Level, Logger}

object TotalSpent extends App{

  //Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "TotalSpent")
  val input = sc.textFile("/Users/saichandchivukula/Desktop/Datasets/customerorders-201008-180523.csv")
  //in the above dataset there are three olumns seperated by comma, first column is user_id and second is product_id and third is amount for the product

  val mappedInput= input.map(x => (x.split(",")(0),x.split(",")(2).toFloat))
  val totalByCustomer = mappedInput.reduceByKey((x,y) => x+y)
  val premiumCustomers = totalByCustomer.filter(x => x._2 > 5000)
  val doubledAmount = premiumCustomers.map(x => (x._1, x._2 * 2))
  doubledAmount.collect().foreach(println)

  println(doubledAmount.count())
  //val sortedTotal = totalByCustomer.sortBy(x=> x._2)

  //val result = sortedTotal.collect()
  //result.foreach(println)
  // the above two commands are used to display the results on the output console, what if we want to store the output in a file
  //sortedTotal.saveAsTextFile("Users/saichandchivukula/Desktop/Datasets/OutputFiles")

  scala.io.StdIn.readLine()
}
