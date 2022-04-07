//1. Atleast 1000 people should have rated that movie
//2. Find all the movie name with the average rating > 4.5
import org.apache.spark.SparkContext
import org.apache.log4j.{Level, Logger}

object TopMovies extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]", "TopMovies")

  val ratingsRdd = sc.textFile("/Users/saichandchivukula/Desktop/Datasets/ratings-201019-002101.dat")
  val mappedRdd = ratingsRdd.map(x => {
    val fields = x.split("::")
    (fields(1), fields(2))
  })
  //(1193,5)
  //(1193,3)
  //(1193,4)
  //Output for above we want is (1193, (5,1))
  //(1193, (4,1)
  //(1193, (3,1))

  val newMapped = mappedRdd.mapValues(x => (x.toFloat, 1.0))
  //Expected output(1193, (12.0, 3.0))
  val reducedRdd = newMapped.reduceByKey((x,y) => (x._1+y._1, x._2+y._2))
  //input (1193, (12.0, 3.0))
  val filteredRdd = reducedRdd.filter(x => x._2._2 > 10)
  // input(1193, (12000.0, 3000.0))
  //output(1193, 4)
  val processedRatings = filteredRdd.mapValues(x => x._1/x._2).filter(x => x._2 > 4.0)

//  processedRatings.collect.foreach(println)

  val moviesRdd = sc.textFile("/Users/saichandchivukula/Desktop/Datasets/movies-201019-002101.dat")
  val mappedMovies = moviesRdd.map(x => {
    val fields = x.split("::")
    (fields(0), fields(1))
  })

  val joinedRdd = mappedMovies.join(processedRatings)
  val topMoviesRdd = joinedRdd.map(x => x._2._1)

  topMoviesRdd.collect().foreach(println)

  scala.io.StdIn.readLine()
}
