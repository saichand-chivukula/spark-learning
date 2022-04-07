import org.apache.spark.SparkContext
import org.apache.log4j.{Level, Logger}

// Here we need to find the how many times movies are rated with 5 stars, 4 stars, 3 stars, 2 stars and 1 star
object MoviesRating extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "MoviesRating")
  val input = sc.textFile("/Users/saichandchivukula/Desktop/Datasets/moviedata-201008-180523.data")
  //in the above dataset there are 4 columns, first column is user_id, second is movie_id, third is rating_given, fourth is time stamp(after Jan 1, 1970)

  val mappedInput = input.map(x => x.split("\t")(2))

  //instead of using map where we say(x,1) and then using the reduceByKey later, same can be achieved using countByValue()
  //map and reduceByKey are transformations and they will be RDD but countByValue is a Action
  //If we feel that countByValue is the last thing you are doing and there are no more operations after that then it is okay to use
  //But if we feel that we need more operations after that then we should not use countByValue, reason is it will be a local not able to achieve parllelism
  val results = mappedInput.countByValue()

  //val mappedCount = mappedInput.map(x => (x,1))
  //val countKey    = mappedCount.reduceByKey((x,y) => x+y)

  //val localResults = countKey.collect()
  // RDD is nothing but the values are gathered in different machines on the cluster which is done in transformations
  // Collections is present on the local machine after bringing the values from the different machines in the cluster

  //localResults.foreach(println)

  results.foreach(println)

}
