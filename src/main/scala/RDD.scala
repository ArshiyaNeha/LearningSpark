import creating_SparkSession.spark

object RDD extends App
{
  //Spark Create RDD from Seq or List (using Parallelize)
  val rdd=spark.sparkContext.parallelize(Seq(("Java", 20000),
    ("Python", 100000), ("Scala", 3000)))
  rdd.foreach(println)

  //Create an RDD from a text file
  val rdd1 = spark.sparkContext.textFile("/path/textFile.txt")





}
