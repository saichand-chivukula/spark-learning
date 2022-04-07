import org.apache.spark.SparkContext
import org.apache.log4j.{Level, Logger}

object WarnError extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]", "WarnError")

  //1. First we need to create the list to traverse through
  val myList = List("WARN: Fri Oct 17 10:37:51 BST 2014",
                    "ERROR: Wed Jul 01 10:37:51 BST 2015",
                    "WARN: Thu Jul 27 10:37:51 BST 2017",
                    "WARN: Thu Oct 19 10:37:51 BST 2017",
                    "WARN: Wed Jul 30 10:37:51 BST 2014")

  //2. Now we need to create the RDD using above List
  // To do so we need to use the parllelize if we are having data in local
  val listToRdd = sc.parallelize(myList)

  val pairedRDD = listToRdd.map(x => {
    val splitCols =  x.split(":")
    val logLevel = splitCols(0)
    (logLevel, 1)
  })

  val logCount = pairedRDD.reduceByKey((x,y) => x + y)

  logCount.collect().foreach(println)

}
