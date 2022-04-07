import org.apache.spark.SparkContext
import org.apache.log4j.{Logger, Level}

object BigDataCampagin extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]", "BigDataCampagin")

  //First we need to read the input and then store in a variable, it stores in the form of RDD
  val input = sc.textFile("/Users/saichandchivukula/Desktop/Datasets/bigdatacampaigndata-201014-183159.csv")
  //We need to select only the values which we are interested in the above we are interested in the columns 1 and 11
  //We need to use map to map the values as tuples with the designated columns as above
  val intColumns = input.map(x => (x.split(",")(10).toFloat, x.split(",")(0)))
  //In the above map we took the columns 10 and 0 in reverse order because we need to seperate each word for this there is a seperate function called as flatMapValues which divides the value present into a single word each
  val flatWordsReversed = intColumns.flatMapValues(x => x.split(" ") )
  //Reverse the key-value pair
  val flatWords = flatWordsReversed.map(x => (x._2.toLowerCase(), x._1))
  // We need to calculate the cost for each word
  val aggWordCost = flatWords.reduceByKey((x,y) => (x+y))
  // Here we are sorting the data based on the values and making sure it is displaying in reverse order i.e. descending
  val sortedWords = aggWordCost.sortBy(x => x._2, false)

  //Action to collect the data and display in the output console
  //sortedWords.collect().foreach(println)
  // if we want to display only top 20 we need to use the take instead of collect
  sortedWords.take(20).foreach(println)
}
