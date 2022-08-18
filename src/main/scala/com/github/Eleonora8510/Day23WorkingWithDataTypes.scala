package com.github.Eleonora8510


import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{bround, col, corr, countDistinct, expr, lit, max, mean, not, pow, round}

object Day23WorkingWithDataTypes extends App {
  println("Ch6: Working with Different Types\nof Data - Part2")
  val spark = SparkUtil.getSpark("BasicSpark")

  val filePath = "src/resources/retail-data/by-day/2010-12-01.csv"
  val df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(filePath)

  df.show(5)
  df.printSchema()
  df.createOrReplaceTempView("dfTable")

  val priceFilter = col("UnitPrice") > 600
  val descriptionFilter = col("Description").contains("POSTAGE")
  df.where(col("StockCode").isin("DOT")).where(priceFilter.or(descriptionFilter))
    .show()

  spark.sql("SELECT * FROM dfTable " +
    "WHERE StockCode IN ('DOT') AND (UnitPrice > 600 OR Description LIKE '%POSTAGE%')")
    .show()

  val DOTCodeFilter = col("StockCode") === "DOT"

  df.withColumn("stockCodeDOT", DOTCodeFilter).show(10)
  df.withColumn("stockCodeDOT", DOTCodeFilter)
    .where("stockCodeDOT")
  .show()// so filters by existence of truth in this column

  df.withColumn("isExpensive", DOTCodeFilter.and(priceFilter.or(descriptionFilter)))
    .where("isExpensive")
    .select("unitPrice", "isExpensive").show(5)

  df.withColumn("isExpensive", not(col("UnitPrice").leq(250)))//less or equal
    .filter("isExpensive")
    .select("Description", "UnitPrice").show(5)

  df.withColumn("isExpensive", expr("NOT UnitPrice <= 250"))
    .filter("isExpensive")
    .select("Description", "UnitPrice").show(5)

  df.withColumn("isExpensive", col("UnitPrice") > 250)
    .filter("isExpensive")
    .select("Description", "UnitPrice")
    .show(5)

//  Warning
//  One “gotcha” that can come up is if you’re working with null data when creating Boolean expressions.
//  If there is a null in your data, you’ll need to treat things a bit differently.
//  Here’s how you can ensure that you perform a null-safe equivalence test:

  //so if Description might have null / does not exist then we would need to use this eqNullSafe method

  df.where(col("Description").eqNullSafe("hello")).show()

  //Working with numbers

  val fabricatedQuantity = pow(col("Quantity") * col("UnitPrice"), 2) + 5
  df.select(col("CustomerId"),col("UnitPrice"), fabricatedQuantity.alias("realQuantity"))
    .show(3)

  df.selectExpr(
    "CustomerId",
    "Quantity",
    "UnitPrice",
    "ROUND((POWER((Quantity * UnitPrice), 2.0) + 5), 2) as realQuantity")
    .show(4)


  df.select(round(col("UnitPrice"), 1).alias("rounded"), col("UnitPrice")).show(5)

  df.select(round(lit("2.6")), bround(lit("2.6"))).show(2)
  df.select(round(lit("2.5")), bround(lit("2.5"))).show(2)
  df.select(round(lit("2.4")), bround(lit("2.4"))).show(2)

  //Pearson correlation
  //https://en.wikipedia.org/wiki/Pearson_correlation_coefficient
  val corrCoefficient = df.stat.corr("Quantity", "UnitPrice") //you can save this single double value as well
  println(s"Pearson correlation coefficient for Quantity and UnitPrice is $corrCoefficient")
  df.select(corr("Quantity", "UnitPrice")).show()

  df.describe().show() //stddev, mean, min, max

  df.select(mean("Quantity"), mean("UnitPrice"), max("UnitPrice")).show()

  val colName = "UnitPrice"
  val quantileProbs = Array(0.1, 0.4, 0.5, 0.6, 0.9, 0.99)
  val relError = 0.05
  val quantilePrice = df.stat.approxQuantile("UnitPrice", quantileProbs, relError) // 2.51

  for ((prob, price) <- quantileProbs zip quantilePrice){
    println(s"Quantile ${prob} - price ${price}")
  }

  val relError_01 = 0.01
  val quantilePrice_01 = df.stat.approxQuantile("UnitPrice", quantileProbs, relError_01) // 2.51

  for ((prob, price) <- quantileProbs zip quantilePrice_01){
    println(s"Quantile ${prob} - price ${price}")
  }

  //https://en.wikipedia.org/wiki/Quantile

  //so default is quartile (4 cut points)
  def getQuantiles(df: DataFrame,
                   colName: String,
                   quantileProbs: Array[Double]=Array(0.1, 0.4, 0.5, 0.6, 0.9, 0.99),
                   relError: Double = 0.05): Array[Double] = {
    df.stat.approxQuantile(colName, quantileProbs, relError)

  }

  def printQuantiles(df: DataFrame,
                     colName: String,
                     quantileProbs: Array[Double]=Array(0.1, 0.4, 0.5, 0.6, 0.9, 0.99),
                     relError: Double = 0.05): Unit = {
    val quantiles = getQuantiles(df,colName,quantileProbs, relError)
    println(s"For column $colName")
    for ((prob, cutPoint) <- quantileProbs zip quantiles){
      println(s"Quantile ${prob} so approx${prob*100}% of data is covered -  cutPoint ${cutPoint}")
    }
  }

  val deciles = (1 to 10).map(n => n.toDouble/10).toArray
  printQuantiles(df, "UnitPrice", deciles)

  val ventiles = (1 to 20).map(n => n.toDouble/20).toArray
  printQuantiles(df, "UnitPrice", ventiles)

  //https://en.wikipedia.org/wiki/Contingency_table
  df.stat.crosstab("StockCode", "Quantity").show()

//  df.groupBy("Country").count().show()
//  df.agg(countDistinct("Country")).show()
//
//  for (col <-df.columns){
//    val count = df.agg(countDistinct(col))
//    //this would get you breakdown by distinct value
//    //val count = df.groupBy(col).count().collect().head.getString(0)
//    println(s"Column $col has $count distinct values")
//  }





}
