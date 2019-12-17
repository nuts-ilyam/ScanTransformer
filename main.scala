import org.apache.spark.sql.{DataFrame, SparkSession}

object main extends App {

  val spark: SparkSession = getSparkSession

  import spark.implicits._

  val parquet = spark.read
    .json("...")


  val regexps = spark.read
    .option("delimiter", ",")
    .option("header", "true")
    .csv("...\\regex.csv")

  val inputCol = /*...*/

  val myTransformer = new ScanRegexTransformer("myTransformer")
    .setInputCol(inputCol)
    .setInputDataFrameWithRegex(regexps)


  def getSparkSession: SparkSession = {
    /*....*/
    spark
  }

}