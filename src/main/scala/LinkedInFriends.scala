import org.apache.spark.SparkContext
import org.apache.log4j.{Level, Logger}

object LinkedInFriends extends  App {

  def parseLine(line: String) ={
    val fields = line.split(",")
    val age = fields(2).toInt
    val numFriends = fields(3).toInt
    (age, numFriends)
  }

  Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "LinkedInFriends")
    val input = sc.textFile("/Users/saichandchivukula/Desktop/Datasets/friendsdata-201008-180523(1).csv")
  //The above dataset consists of 4 columns i.e. row_id, name, age , no of connections on LinkedIn


   val mappedInput   = input.map(parseLine)
   //val mappedFriends = mappedInput.map(x => (x._1,(x._2, 1))) alternate is below
   val mappedFriends = mappedInput.mapValues(x => (x,1))
   val friendsCount  = mappedFriends.reduceByKey((x,y) => (x._1 + y._1, x._2+y._2))
   //val avgFriendsAge = friendsCount.map(x => (x._1, x._2._1/x._2._2)) alternate is below
  val avgFriendsAge = friendsCount.mapValues(x => x._1/x._2)
  avgFriendsAge.collect().foreach(println)
}
