import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructType}

object SelectOperations extends App
{
  val spark = SparkSession.builder
    .master("local[1]")
    .appName("Sparkbyexamples.com")
    .getOrCreate()

  Logger.getLogger("org").setLevel(Level.ERROR)

  val data = Seq(("James","Smith","USA","CA"),
    ("Micheal","Rose","USA","NY"),
    ("Robert","Williams","USA","CA"),
    ("Maria","Jones","USA","FL")
  )

  val columns = Seq("firstname","lastname","country","state")

  import spark.implicits._
  val df = data.toDF(columns:_*)
  df.show(false)

  //select operations
  df.select("firstname","lastname").show()

  df.select("*").show()

  //to select 4th column
  df.select(df.columns(3)).show()

  //select columns from index 2 to 4
  df.select(df.columns.slice(2,4).map(c => col(c)):_*).show()

  //select columns that starts and ends with some name
  df.select(df.columns.filter(f => f.startsWith("first")).map(c => col(c)):_*).show()
  df.select(df.columns.filter(f => f.endsWith("name")).map(c => col(c)):_*).show()


  //nested columns
  val data2 = Seq(Row(Row("James","","Smith"),"OH","M"),
    Row(Row("Anna","Rose",""),"NY","F"),
    Row(Row("Maria","Anne","Jones"),"NY","M"),
    Row(Row("Jen","Marry","Brown"),"NY","M")
  )
  val schema = new StructType()
    .add("name" ,new StructType()
      .add("firstname",StringType)
      .add("middlename",StringType)
      .add("lastname",StringType))
    .add("State",StringType)
    .add("Gender",StringType)


  val df2 = spark.createDataFrame(spark.sparkContext.parallelize(data2),schema)
  df2.printSchema()
  df2.show(false)

  df2.select("name").show()
  df2.select("name.firstname","name.lastname").show(false)
  df2.select("name.*").show()

}
