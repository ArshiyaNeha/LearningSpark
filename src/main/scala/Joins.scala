import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Joins extends App
{
  val spark : SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("mama.org")
    .getOrCreate()

  Logger.getLogger("org").setLevel(Level.ERROR)

  val emp = Seq((1,"sameer",-1,"2018","10","Male",3000),
    (2,"Suheab",1,"2010","20","Male",4000),
    (3,"Williams",1,"2010","10","Male",1000),
    (4,"Arshi",2,"2005","10","Female",2000),
    (5,"Brown",2,"2010","40","",-1),
    (6,"Brown",2,"2010","50","",-1)
  )

  //creating emp table using seq
  val empColumns = Seq("emp_id","name","superior_emp_id","year_joined","emp_dept_id","gender","salary")

  import spark.sqlContext.implicits._
  val empDF = emp.toDF(empColumns:_*)
  empDF.show()

  val dept = Seq(("Finance",10),
    ("Marketing",20),
    ("Sales",30),
    ("IT",40)
  )


  val deptColumns = Seq("dept_name","dept_id")
  val deptDF = dept.toDF(deptColumns:_*)
  deptDF.show(false)

  //join operations
  empDF.join(deptDF,empDF("emp_dept_id") === deptDF("dept_id"), "inner").show()

  empDF.join(deptDF,empDF("emp_dept_id") ===  deptDF("dept_id"),"outer")
    .show(false)
  empDF.join(deptDF,empDF("emp_dept_id") ===  deptDF("dept_id"),"full")
    .show(false)
  empDF.join(deptDF,empDF("emp_dept_id") ===  deptDF("dept_id"),"fullouter")
    .show(false)
}
