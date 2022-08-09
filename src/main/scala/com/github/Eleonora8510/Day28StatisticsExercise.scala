package com.github.Eleonora8510

import com.github.Eleonora8510.Day28StatisticalFun.df
import com.github.Eleonora8510.SparkUtil.{getSpark, readDataWithView}
import org.apache.spark.sql.functions.{collect_set, corr, covar_pop, kurtosis, mean, skewness, stddev_pop, stddev_samp, var_pop, var_samp}

object Day28StatisticsExercise extends App {
  println("Let's make some stats on one day data")

  val spark = getSpark("Sparky")
  val filePath = "src/resources/retail-data/by-day/2011-03-08.csv"
  val df = readDataWithView(spark, filePath)

  df.show(5)

  //TODO
  //load March 8th of 2011 CSV
  //lets show avg, variance, std, skew, kurtosis, correlation and population covariance

  df
    .select(
      mean("UnitPrice"),
      var_pop("UnitPrice"),
      var_samp("UnitPrice"),
      stddev_pop("UnitPrice"),
      stddev_samp("UnitPrice"),
      skewness("UnitPrice"),
      kurtosis("UnitPrice"),
      corr("UnitPrice", "Quantity"),
      covar_pop("UnitPrice", "Quantity")
    )
    .show()


  spark.sql(
    """
      |SELECT
      |mean(Quantity),
      |var_pop(Quantity),
      |var_samp(Quantity),
      |stddev_pop(Quantity),
      |stddev_samp(Quantity),
      |skewness(Quantity),
      |kurtosis(Quantity),
      |corr(Quantity, UnitPrice),
      |covar_pop(Quantity, UnitPrice)
      |FROM dfTable
      |""".stripMargin
  ).show()

//  df
//    .select(
//      mean("Quantity"),
//      var_pop("Quantity"),
//      var_samp("Quantity"),
//      stddev_pop("Quantity"),
//      stddev_samp("Quantity"),
//      skewness("Quantity"),
//      kurtosis("Quantity"),
//      corr("Quantity", "UnitPrice"),
//      covar_pop("Quantity", "UnitPrice")
//    )
//    .show()

  //TODO transform unique Countries for that day into a regular Scala Array of strings

  //you could use SQL distinct of course - do not hav eto use collect_set but you can :)

  df.agg(collect_set("Country")).collect().foreach(println)

//  val countries = spark.sql(
//    """
//      |SELECT DISTINCT(Country)
//      |FROM dfTable
//      |""".stripMargin
//  )


}
