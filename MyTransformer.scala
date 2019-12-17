
/*
* import spark: SparkSession from main
*
*/

import main.spark

import spark.implicits._
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.types._
import scala.util.matching.Regex

/*
*two Transformer params :
* 1. DataFrame with regexps
* 2. The name of the column in which to look for this regexps
*/


class ScanRegexTransformer(override val uid: String) extends Transformer {
  final val inputCol: Param[String] = new Param[String](this, "inputCol", "input column name")

  final def getInputCol: String = $(inputCol)

  final def setInputCol(value: String): ScanRegexTransformer = set(inputCol, value)

  final val inputDataFrameWithRegex: Param[DataFrame] = new Param[DataFrame](this, "inputDFWithRegex", "DataFrame with regex")

  final def getInputDataFrameWithRegex: DataFrame = $(inputDataFrameWithRegex)

  final def setInputDataFrameWithRegex(value: DataFrame): ScanRegexTransformer = set(inputDataFrameWithRegex, value)

  override def transform(dataset: Dataset[_]): DataFrame = {

    def FilterRegex(data: String, regexp: String) = {
      val pattern = new Regex(regexp)
      !(pattern findFirstIn data).mkString(",").isEmpty()
    }

    spark.udf.register("FilterFunction", FilterRegex _)

    var output = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], dataset.schema)

    val dfWithRegex = $(inputDataFrameWithRegex).select($"regexp", $"date", unix_timestamp($"date", "dd.MM.yyyy").cast(TimestampType).as("timestamp"))
      .select($"regexp",
        date_add($"timestamp", -1).cast(TimestampType).as("days_before"),
        date_add($"timestamp", 2).cast(TimestampType).as("days_after"))


    val RegsDates = dfWithRegex.select("regexp", "days_before", "days_after").rdd.map(r => (r(0), r(1), r(2))).collect()


    for ((cur_regs, days_before, days_after) <- RegsDates) {
      val currentData = dataset.filter(col("date") > days_before and col("date") < days_after
        and callUDF("FilterFunction", col($(inputCol)), lit(cur_regs)))

      output = output.union(currentData.select($"*").toDF())
    }
    output
  }

  override def transformSchema(schema: StructType): StructType = {
    val actualColumnDataType = schema($(inputCol)).dataType
    require(actualColumnDataType.equals(DataTypes.StringType),
      s"Column ${$(inputCol)} must be StringType but was actually $actualColumnDataType .")
    schema
  }

  override def copy(extra: ParamMap): ScanRegexTransformer = defaultCopy(extra)

}
