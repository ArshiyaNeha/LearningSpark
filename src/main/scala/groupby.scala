import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, sum}

object groupby extends App
{
  val spark : SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("abc.org")
    .getOrCreate()

  Logger.getLogger("org").setLevel(Level.ERROR)

  //creating the simple data
  val simpleData = Seq(("Arshiya","Neha","New york",90000,34,10000),
    ("MD","Sameer","New york",86000,56,20000),
    ("Kruthika","S","Calicut",81000,30,23000),
    ("swathi","A","Calicut",90000,24,23000),
    ("Tabinda","H","Calicut",99000,40,24000),
  )

  import spark.implicits._
  //converting seq data into Data Frame
  val df = simpleData.toDF("employee_name","department","state","salary","age","bonus")
  df.show()

    //GroupBy operations
  df.groupBy("department").sum("salary").show(false)
  df.groupBy("department").count().show()
  df.groupBy("department").min("salary").show()
  df.groupBy("department").max("salary").show()
  df.groupBy("department").mean( "salary").show()


  df.groupBy("department","state").sum("salary","bonus").show(false)

  //running more aggregate at a time
  df.groupBy("department")
    .agg(sum("salary").as("sum_salary"),
      avg("salary").as("avg_salary")).show()

  df.groupBy("department")
    .agg(sum("salary").as("sum_salary"),
      avg("salary").as("avg_salary")).where(col("sum_bonus")>= 50000).show(false)


}
