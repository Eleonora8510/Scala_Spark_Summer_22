package com.github.Eleonora8510.Final_project

import com.github.Eleonora8510.SparkUtil.getSpark
import org.apache.spark.sql.functions._

object StockAnalysis extends App{
  val spark = getSpark("StockMarketAnalysis")

  val defaultSrc = "src/resources/final_project/stock_prices_.csv"
  val filePath = if (args.length >= 1) args(0) else defaultSrc
  println(s"READING FILE FROM $filePath")

  //Load up stock_prices.csv as a DataFrame
  val df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(filePath)

  df.show(5, false)

  val dateFormat = "yyyy-MM-dd"
  val dfWithDate = df
    .withColumn("date", to_date(col("date"), dateFormat))
    .withColumn("dailyReturn", expr("round((close - open) / open * 100, 4)" ))//daily change in percentages for all rows

  println("ADDED COLUMN WITH DAILY RETURN - CHANGE IN %")
  dfWithDate.sort("date").show(10)

  dfWithDate.createOrReplaceTempView("dfWithDateView")

  println("AVERAGE DAILY RETURN IN % (SHOWING EVERY STOCK)")

  //Show individual ticker daily return and compute the average daily return of all stocks combined
  val avgValuesDf = spark.sql(
    """
      |SELECT
      |date, ticker, dailyReturn,
      |AVG(dailyReturn) OVER (PARTITION BY date) as avgDailyReturn
      |FROM dfWithDateView
      |WHERE date IS NOT NULL
      |ORDER BY date
      |""".stripMargin)

  //Create dataframe with avgDailyReturn rounded
  val avgValuesDfRounded = avgValuesDf
    .withColumn("avgDailyReturn", expr("ROUND(`avgDailyReturn`, 4)"))
    .select("date", "ticker", "dailyReturn", "avgDailyReturn")
  avgValuesDfRounded.show(10)

  //Compute the daily average return of all stocks combined, without individual ticker daily return
  println("AVERAGE DAILY RETURN IN % OF ALL STOCKS COMBINED")
  val avgValuesDf1 = dfWithDate
    .groupBy("date")
    .agg(round(avg("dailyReturn"), 4).alias("avgDailyReturn"))
    .sort("date")

  avgValuesDf1.show(10)

  //Save the results to the file as Parquet
  //If file already exists, it will be overwritten with updated data
  avgValuesDf1.write
    .format("parquet")
    .mode("overwrite")
    .save("src/resources/final_project/average_stock_returns.parquet")

  //Save the results to the file as CSV
  //If file already exists, it will be overwritten with updated data
  avgValuesDf1.write
    .format("csv")
    .mode("overwrite")
    .option("header", true)
    .save("src/resources/final_project/average_stock_returns.csv")

  val newPath = "jdbc:sqlite:src/resources/final_project/final-sqlite.db"
  val tableName = "Stock_prices"

  println(s"WRITING TO SQL DATABASE $newPath TABLE $tableName")

  val props = new java.util.Properties
  props.setProperty("driver", "org.sqlite.JDBC")

  //Cast date type to string
  val avgValuesDf2 = avgValuesDf1
    .withColumn("date", col("date").cast("string"))
    .select("date", "avgDailyReturn")

  //Save the results to SQL database
  avgValuesDf2
    .write
    .mode("overwrite")
    .jdbc(newPath, tableName, props)

  //Calculates stock frequency - measured by closing price * volume - on average
  val mostFrequentStocks = spark.sql(
    """
      |SELECT ticker, ROUND((SUM(close * volume)/COUNT(volume))/1000,2)
      |AS avgFrequencyThousands
      |FROM dfWithDateView
      |GROUP BY ticker
      |ORDER BY avgFrequencyThousands DESC
      |""".stripMargin)

  println("MOST FREQUENT TRADED STOCK ON AVERAGE")
  mostFrequentStocks.show(1)

  //Calculates stock volatility - measured by annualized standard deviation of daily returns
  //Formula -> standard deviation of daily returns * square root of trading days per year

  val tradingDays = dfWithDate.selectExpr("count(distinct(date))").first.getLong(0) //getting the count of trading days - 249
  println("THE MOST VOLATILE STOCK")
  dfWithDate
    .groupBy("ticker")
    .agg((stddev(col("dailyReturn")) * math.sqrt(tradingDays)).alias("annualizedStdDev"))
    .orderBy(desc("annualizedStdDev"))
    .show(1)


}
