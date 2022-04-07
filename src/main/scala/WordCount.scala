import org.apache.spark.SparkContext
import org.apache.log4j.{Level, Logger}

object WordCount extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "wordcount")

  val input = sc.textFile("/Users/saichandchivukula/Desktop/Datasets/search_data.txt")

  val words = input.flatMap(x => x.split(" ") )

  val words_lower = words.map(x => x.toLowerCase())

  val mapped_words = words_lower.map(x => (x,1) )

  val word_count = mapped_words.reduceByKey((x,y) => x+y)

  val key_count = word_count.map(x => (x._2, x._1))

  val key_top_10 = key_count.sortByKey(false)//By default it sorts based on Ascending order

  val org_order = key_top_10.map(x => (x._2, x._1))

  org_order.collect().foreach(println)

  scala.io.StdIn.readLine()

}
