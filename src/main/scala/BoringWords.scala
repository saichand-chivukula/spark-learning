import org.apache.spark.SparkContext
import org.apache.log4j.{Level, Logger}
import scala.io.Source

object BoringWords extends App {
  //this code runs on the driver in our case on our local machine but not parllely on distributed systems
  //creating the function to read the boring values words into set. We are using set because set does not accept duplicates
  def loadBoringWords( ):Set[String] = {
    // we are creating the empty string
    var boringWords: Set[String] = Set()
    // reading the data from the local machine with the help of Source module from Scala.io
    // getLines() will read all the lines
    val fileToLoad = Source.fromFile("/Users/saichandchivukula/Desktop/Datasets/boringwords.txt").getLines()
    // we are traversing through the lines of the file
    for(line <- fileToLoad){
      boringWords += line
    }
    // here we are returing the set of boring words to the fuunction
    boringWords
  }

  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]", "BoringWords")

  // we are sending the file to the local machine because we need to optimize the process with the help of broadcast variable
  var nameSet = sc.broadcast(loadBoringWords())

  val input = sc.textFile("/Users/saichandchivukula/Desktop/Datasets/bigdatacampaigndata-201014-183159.csv")
  val mappedInput = input.map(x => (x.split(",")(10).toFloat, x.split(",")(0)))
  val words = mappedInput.flatMapValues(x => x.split(" "))
  val finalMapped = words.map(x => (x._2.toLowerCase(), x._1))

  // here we are filtering the values to the non boring word list by using the negation operator
  val filteredRDD = finalMapped.filter(x => !nameSet.value(x._1))

  val total = filteredRDD.reduceByKey((x,y) => x+y)
  val sorted = total.sortBy(x => x._2, false)

  sorted.take(20).foreach(println)


}
