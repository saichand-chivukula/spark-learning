import org.apache.spark.SparkContext
import org.apache.log4j.{Level, Logger}

object CalculatingBlankLines extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]", "CalculatingBlankLines")

  val inputRDD = sc.textFile("/Users/saichandchivukula/Desktop/Datasets/samplefile.txt")
  val myAccumulator = sc.longAccumulator("my accumulator to calculate blank lines")
  val blankLines = inputRDD.foreach(x => if (x == "") myAccumulator.add(1))

  val accumulatorValue = myAccumulator.value

}
