import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.array_contains
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object Filter extends App
{
  Logger.getLogger("org").setLevel(Level.ERROR)

  //creating spark session
  val spark : SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("abc.com")
    .getOrCreate()

  //creating a data
  val CreateData = Seq(
    Row(Row("Arshiya","","Neha"),List("Java","Scala","C++")," Karnataka","Female"),
    Row(Row("Taiyyaba","Neha",""),List("Spark","Java","C++"),"Maharastra","Female"),
    Row(Row("Kruthika","chidambar","sidnal"),List("CSharp","VB"),"Tamil Nadu","Male"),
    Row(Row("Swathi","A","u"),List("CSharp","VB"),"Punjab","Male"),
  )


  //create structure of schema
  val creatinschema = new StructType()
    .add("name",new StructType()
      .add("firstname",StringType)
      .add("middlename",StringType)
      .add("lastname",StringType))
    .add("languages", ArrayType(StringType))
    .add("state", StringType)
    .add("gender", StringType)

  val df = spark.createDataFrame(
    spark.sparkContext.parallelize(CreateData),creatinschema)
  df.printSchema()
  df.show()

  //filters
  df.filter(df("state")==="Maharastra" ).show(false)
  df.filter(array_contains(df("languages"),"Java")).show()

  //filter on nested column structures
  df.filter(df("name.lastname")==="sidnal").show(false)

}
