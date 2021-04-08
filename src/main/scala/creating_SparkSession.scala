import org.apache.spark.sql.SparkSession

object creating_SparkSession extends App
{
  //creating spark session
  val spark:SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("learning")
    .getOrCreate()
  /*
    .builder() = use the builder pattern method
    .master() = If you are running it on the cluster you need to use your master name as an argument to master() ,
    local[x] = when running in Standalone mode. x should be an integer value and should be greater than 0; this represents how many partitions it should create when using RDD, DataFrame, and Dataset.
    Ideally, x value should be the number of CPU cores you have.
    .appName =  Used to set your application name.
    .getOrCreate() = This returns a SparkSession object if already exists, creates new one if not exists.
   */
  //Create RDD from parallelize
  val dataSeq = Seq(("Java", 20000), ("Python", 100000), ("Scala", 3000))
  val rdd=spark.sparkContext.parallelize(dataSeq)//sparkContext.parallelize is used to parallelize an existing collection in your driver program.


}
