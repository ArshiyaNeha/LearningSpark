import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SqlDistinct extends App
{
  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("abc.com")
    .getOrCreate()

  Logger.getLogger("org").setLevel(Level.ERROR)


  val simData = Seq(("James","Sales",3000),
    ("kartik","Sales",2500),
    ("Maria","Finance",5000),
    ("James","Sales",5000),
    ("Scott","finance",8000),
    ("kumar","marketing",8900),
    ("jen","finance",3500))

  import spark.implicits._
  val df = simData.toDF("employeename","dept","salary")
  df.show()

  val distinctDf = df.distinct()
  println("Distinct Count :"+ distinctDf.count())
  distinctDf.show()



}
