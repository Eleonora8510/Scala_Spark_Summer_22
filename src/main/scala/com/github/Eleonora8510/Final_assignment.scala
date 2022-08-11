package com.github.Eleonora8510

import com.github.Eleonora8510.SparkUtil.{getSpark, readDataWithView}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, desc, expr, max, mean, round, to_date}

object Final_assignment extends App {
  println("Final Project - Stock Price Analysis")
  val spark = getSpark("Spark")

  val pathFile = "src/resources/final_project/stock_prices_.csv"
  val df = readDataWithView(spark, pathFile)

  df.show(5, false)

  val dateFormat = "yyyy-MM-dd"

  val dfWithDate = df
    .withColumn("date", to_date(col("date"), dateFormat))
    //.select(to_date(col("date"), dateFormat)).alias("date")
    .withColumn("dailyReturn", expr("round((close - open) / open * 100, 4)" ))

    dfWithDate.show(10)

  dfWithDate.createOrReplaceTempView("dfWithDateView")

    spark.sql(
      """
        |SELECT
        |date, dailyReturn,
        |AVG(dailyReturn) OVER (PARTITION BY date) as avgDailyReturn
        |FROM dfWithDateView
        |WHERE date IS NOT NULL
        |ORDER BY date DESC
        |""".stripMargin
    ).show()


//  val windowSpec = Window
//    .partitionBy("date")
////    .orderBy(col("daily return"))
////    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
//
//  val avgDailyReturn = round(avg(col("dailyReturn")), 4).over(windowSpec)
////  val maxDailyReturn = max(col("daily return")).over(windowSpec)
//
//  dfWithDate
//    //.where("date IS NOT NULL")
//    .orderBy(desc("date"))
//    .select(col("date"),
//      //col("ticker"),
//      col("dailyReturn"),
////    maxDailyReturn.alias("max daily return"))
//  avgDailyReturn.alias("average daily return"))
//    .show(40, false)






//  val dailyReturn = df.withColumn("daily_return", round((col("close") - col("open"))/col("open")*100,3))
//
//  dailyReturn
//    .select("date", "open", "close", "ticker", "daily_return")
//    .sort("date").show(10)
//
//
//  println("RETURN OF ALL STOCKS EACH DAY - AVERAGE DAILY RETURN")
//  val avgDailyReturn = dailyReturn
//    .groupBy("date")
//    .agg(round(avg("daily_return"),3).alias("avg_daily_return"))
//    .sort("date")
//
//  avgDailyReturn.show(10)


}
