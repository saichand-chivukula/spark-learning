import org.apache.spark.SparkContext
import org.apache.log4j.{Level, Logger}

object BigLog extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]", "BigLog")

  val baseRDD = sc.textFile("/Users/saichandchivukula/Desktop/Datasets/bigLog.txt")

  val mappedRDD = baseRDD.map(x => {
    //(x.split(":")(0), x.split(":")(1)))
    val fields = x.split(":")
    (fields(0), fields(1))
  })
  //mappedRDD.groupByKey().collect().foreach(x => println(x._1, x._2.size))

  mappedRDD.reduceByKey((x,y) => x + y).collect().foreach(println)

  scala.io.StdIn.readLine()
}
