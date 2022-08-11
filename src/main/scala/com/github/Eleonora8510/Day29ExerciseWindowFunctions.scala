package com.github.Eleonora8510

import com.github.Eleonora8510.SparkUtil.{getSpark, readDataWithView}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, dense_rank, desc, max, min, rank, to_date}

object Day29ExerciseWindowFunctions extends App {
  println("Window functions with Spark")

  val spark = getSpark("Sparky")

  val filePath = "src/resources/retail-data/all/*.csv"
  //val filePath = "src/resources/retail-data/by-day/2010-12-01.csv"

  val df = readDataWithView(spark, filePath)

  //TODO create WindowSpec which partitions by StockCode and date, ordered by Price
  //with rows unbounded preceding and current row

  //create max min dense rank and rank for the price over the newly created WindowSpec

  //show top 40 results ordered in descending order by StockCode and price
  //show max, min, dense rank and rank for every row as well using our newly created columns(min, max, dense rank and rank)

  //you can use spark api functions
  //or you can use spark sql

  spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
  val dfWithDate = df.withColumn("date", to_date(col("InvoiceDate"), "MM/d/yyyy H:mm"))

  val windowSpec = Window
    .partitionBy("StockCode", "date")
    .orderBy(col("UnitPrice"))
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

  val maxPrice = max(col("UnitPrice")).over(windowSpec)
  val minPrice = min(col("UnitPrice")).over(windowSpec)

  val priceRank = rank().over(windowSpec)
  val priceDenseRank = dense_rank().over(windowSpec)

  dfWithDate.where("StockCode IS NOT NULL")
    //.orderBy(desc("StockCode"), desc("UnitPrice"))
    .select(
      col("date"),
      col("StockCode"),
      col("UnitPrice"),
      maxPrice.alias("max price"),
      minPrice.alias("min price"),
      priceRank.alias("priceRank"),
      priceDenseRank.alias("priceDenseRank"))
    .orderBy(desc("StockCode"), desc("UnitPrice"))
    .show(40, false)

  dfWithDate.createOrReplaceTempView("dfWithDateView")

  spark.sql(
    """
      |SELECT date, StockCode, UnitPrice,
      |rank(UnitPrice) OVER (PARTITION BY StockCode, date
      |ORDER BY StockCode DESC, UnitPrice DESC NULLS LAST
      |ROWS BETWEEN
      |UNBOUNDED PRECEDING AND
      |CURRENT ROW) as rank,
      |dense_rank(UnitPrice) OVER (PARTITION BY StockCode, date
      |ORDER BY StockCode DESC, UnitPrice DESC NULLS LAST
      |ROWS BETWEEN
      |UNBOUNDED PRECEDING AND
      |CURRENT ROW) as dRank,
      |max(UnitPrice) OVER (PARTITION BY StockCode, date
      |ORDER BY StockCode DESC, UnitPrice DESC NULLS LAST
      |ROWS BETWEEN
      |UNBOUNDED PRECEDING AND
      |CURRENT ROW) as maxPrice,
      |min(UnitPrice) OVER (PARTITION BY StockCode, date
      |ORDER BY StockCode DESC, UnitPrice DESC NULLS LAST
      |ROWS BETWEEN
      |UNBOUNDED PRECEDING AND
      |CURRENT ROW) as minPrice
      |FROM dfWithDateView WHERE StockCode IS NOT NULL
      |ORDER BY StockCode DESC, UnitPrice DESC
      |""".stripMargin)
    .show(40, false)

}
