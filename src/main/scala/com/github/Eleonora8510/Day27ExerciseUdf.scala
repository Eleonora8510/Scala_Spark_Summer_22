package com.github.Eleonora8510

import com.github.Eleonora8510.SparkUtil.{getSpark, readDataWithView}
import org.apache.spark.sql.functions.{approx_count_distinct, col, count, countDistinct, expr, udf}

object Day27ExerciseUdf extends App {
  println("User-defined functions and some aggregation functions")

  val spark = getSpark("SparkFun")

  //TODO create a UDF which converts Fahrenheit to Celsius
  //TODO Create DF with column temperatureF with temperatures from -40 to 120 using range or something else if want
  //TODO register your UDF function
  //TODO use your UDF to create temperatureC column with the actual conversion

  //TODO show both columns starting with F temperature at 90 and ending at 110( both included)

  //You probably want Double incoming and Double also as a return

  def temperatureFtoC(t : Double): Double = (t - 32) * 5 / 9

  val temperaturesDF = spark.range(-40, 121).toDF("temperatureF")
  val tempFtoC = udf(temperatureFtoC(_:Double):Double)

  temperaturesDF
    .withColumn("temperatureC", tempFtoC(col("temperatureF")))
    .where(expr("temperatureF >= 90 AND temperatureF <= 110"))
    .show(21)

  // In SQL
  spark.udf.register("tempFtoC_udf", temperatureFtoC(_: Double): Double)
  temperaturesDF.createOrReplaceTempView("tempDfTable")

  spark.sql(
    """
      |SELECT *,
      |tempFtoC_udf(temperatureF) as temperatureC
      |FROM tempDfTable
      |WHERE temperatureF >=90 AND temperatureF <=110
      |""".stripMargin
  ).show(21)

  //TODO simple task find count, distinct count and also aproximate distinct count (with default RSD)
  // for InvoiceNo, CustomerID AND UnitPrice columns
  //of course count should be the same for all of these because that is the number of rows

  val filePath = "src/resources/retail-data/all/*.csv" //here it is a single file but wildcard should still work
  val df = readDataWithView(spark, filePath)

  df.select(
    count("InvoiceNo"),
    countDistinct("InvoiceNo"),
    approx_count_distinct("InvoiceNo"))
    .show()

  df.select(
    count("CustomerID"),
    countDistinct("CustomerID"),
    approx_count_distinct("CustomerID"))
    .show()

  df.select(
    count("UnitPrice"),
    countDistinct("UnitPrice"),
    approx_count_distinct("UnitPrice"))
    .show()

  //in SQL
  spark.sql(
    """
      |SELECT
      |COUNT(InvoiceNo),
      |COUNT(DISTINCT(InvoiceNo)),
      |approx_count_distinct(InvoiceNo)
      |FROM dfTable
      |""".stripMargin
  ).show()

  spark.sql(
    """
      |SELECT
      |COUNT(CustomerID),
      |COUNT(DISTINCT(CustomerID)),
      |approx_count_distinct(CustomerID)
      |FROM dfTable
      |""".stripMargin
  ).show()

  spark.sql(
    """
      |SELECT
      |COUNT(UnitPrice),
      |COUNT(DISTINCT(UnitPrice)),
      |approx_count_distinct(UnitPrice)
      |FROM dfTable
      |""".stripMargin
  ).show()

}
