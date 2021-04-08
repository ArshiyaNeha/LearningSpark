import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object whenOtherWise extends App
{
  Logger.getLogger("org").setLevel(Level.ERROR)


  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("sum.com")
    .getOrCreate()

  val data = List(("Tabinda","","Bhat", "5642",  "F",60000),
    ("Arshiya","Neha","", "4587", "F",50000) ,
    ("Jethu","", "Methu", 7895, "M",64520),
    ("Tyba","Sangien" , "",8542,"F",8000),
    ("Rauful","Azam","",5555,"M",9000))

  val columns = Seq("firstname","middlename","lastname","dob","gender","salary")
  val df = spark.createDataFrame(data).toDF(columns:_*)

  val df2 = df.withColumn("new_gender", when(col("gender") === "M","Male")
    .when(col("gender") === "F","Female")
    .otherwise("Unknown"))

  val df4 = df.select(col("*"),when(col("gender")==="M","Male")
    .when(col("gender")==="F","Female")
    .otherwise("Unknown").alias("new_gender"))
  df4.show()

}
