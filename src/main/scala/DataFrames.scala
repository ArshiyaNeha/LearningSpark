import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object DataFrames extends App
{
  val spark:SparkSession = SparkSession.builder()
    .master("local[1]").appName("learning")
    .getOrCreate()

  Logger.getLogger("org").setLevel(Level.ERROR)

  import spark.implicits._
  val columns = Seq("language","users_count")
  val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))

  val rdd = spark.sparkContext.parallelize(data)
  val rdd1 = rdd.toDF()
  rdd1.printSchema()
  println(rdd1)
  rdd1.select("*").show()
  rdd1.select(col("_1"))
  //rdd1.toDS()
  println("where")
  rdd1.where(col("_1") === "Java").show()
  rdd1.where("true").show()



  val convert = Seq(1,2,3).toDS()
  convert.show()
  Seq("a","b").toDS().show()

  Seq(("a",1),("b",2),("c",3)).toDS().show()


  //createDataFrame() and toDF() methods are two different way's to create DataFrame in spark.
  // By using toDF() method, we don't have the control over schema customization whereas in createDataFrame()
  // method we have complete control over the schema customization.



  val dfFromRdd1 = rdd.toDF("language","users_count")
  dfFromRdd1.printSchema()

  val dfFromRdd2 = spark.createDataFrame(rdd).toDF(columns:_*)

  val a =10
  val b = 20
  println(s"${a+b}")
  val list3 = List(1,2,3)
  val list2 = List(4,5,6)
  println(s"b ")
  val list = List("a",2)

}
