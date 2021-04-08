import org.apache.spark.{SparkConf, SparkContext}

object Creating_SparkContext extends App
{
  //Creating SparkContext
  val sparkConf = new SparkConf().setAppName("sparkbyexamples.com").setMaster("local[1]")
  val sparkContext = new SparkContext(sparkConf )
  SparkContext.getOrCreate(sparkConf)

  //Once you create a Spark Context object, use this to create Spark RDD.
  //val rdd = sparkContext.textFile("/src/main/resources/text/alice.txt")

}
