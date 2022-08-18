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

  val countries = df.agg(collect_set("Country")).collect()

 // val countryStrings = countries.map(_.getString(0)))
  //println(countryStrings.mkString(","))
  println(countries.mkString)
  println("Printing row by row:")

  for (row<-countries){
    println(row)
  }
  ///turns out we only have a single row , so we would split it using regex


    val distinctCountries = spark.sql(
    """
      |SELECT DISTINCT(Country)
      |FROM dfTable
      |""".stripMargin
  )

  distinctCountries.show()
  val countryStringsRows = distinctCountries.collect()
  val countryStrings = countryStringsRows.map(_.getString(0))
  println(countryStrings.mkString(","))

  //this is how we could get out sequence saved in a single cell of dataframe
  //check chapter 6 on complex types
  val countrySeq: Seq[String] = countries.head.getSeq(0) //first column for our first row
  println(countrySeq.mkString("[",",","]"))

}
