package com.github.Eleonora8510

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.functions // lot of functions

object Day18SparkSQL extends App{
  println(s"Reading CSVs with Scala version: ${util.Properties.versionNumberString}")

  val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
  //  spark.sparkContext.setLogLevel("WARN") //if you did not have log4j.xml set up in src/main/resources
  //spark.sparkContext.setLogLevel("WARN") //if you did not have log4j.xml.bak set up in src/main/resources
  //problem with above approach is that it would still spew all the initial configuration debeg info
  println(s"Session started on Spark version ${spark.version}")

//  // in Scala
  val flightData2015 = spark
    .read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/resources/flight-data/csv/2015-summary.csv") //relative path to our project

  println(s"We have ${flightData2015.count()} rows of data")
  println(flightData2015.take(3).mkString(", "))

  //if we want to use SQL syntax on our DataFrames we create a SQL view
  flightData2015.createOrReplaceTempView("flight_data_2015")

  //now we can use SQL!
  // in Scala
  val sqlWay = spark.sql("""
      SELECT DEST_COUNTRY_NAME, count(1)
      FROM flight_data_2015
      GROUP BY DEST_COUNTRY_NAME
      """)

  //this is the other approach
  val dataFrameWay = flightData2015.groupBy("DEST_COUNTRY_NAME").count()

  sqlWay.show(10)

  dataFrameWay.show(10)

  sqlWay.explain()
  dataFrameWay.explain()


  //TODO set up logging  log4j2.xml config file
  //TODO set level to warning for both file and console

  //TODO open up flight Data from 2014
  //TODO create SQL view
  //TODO ORDER BY flight counts
  //TODO show top 10 flights
//
//  val flightData2014 = spark
//    .read
//    .option("inferSchema", "true")
//    .option("header", "true")
//    .csv("src/resources/flight-data/csv/2014-summary.csv")
//
//  flightData2014.createOrReplaceTempView("flight_data_2014")
//
//  val sqlWay2014 = spark.sql("""
//      SELECT DEST_COUNTRY_NAME, SUM(count) as flight_counts
//      FROM flight_data_2014
//      GROUP BY DEST_COUNTRY_NAME
//      ORDER BY flight_counts DESC
//      """)
//
//  sqlWay2014.show(10)
//
//  val dataFrameWay2014 = flightData2014.groupBy("DEST_COUNTRY_NAME").sum("count").sort(desc("sum(count)"))
//
//  dataFrameWay2014.show(10)
//
  //again two approaches to do the same thing
  spark.sql("SELECT max(count) from flight_data_2015").show()//show everything in this case just 1 row
  flightData2015.select(functions.max("count")).show()//intellij feels that functions.max would be less confusing

//  //very similar to the exercise except we use limit 5 here
//  val maxSql = spark.sql("""
//      SELECT DEST_COUNTRY_NAME, sum(count) as destination_total
//      FROM flight_data_2015
//      GROUP BY DEST_COUNTRY_NAME
//      ORDER BY sum(count) DESC
//      LIMIT 5
//      """)
//  maxSql.show()
//
//  //Now, let’s move to the DataFrame syntax that is semantically similar but slightly different in
//  //implementation and ordering
//  flightData2015
//    .groupBy("DEST_COUNTRY_NAME")
//    .sum("count")
//    .withColumnRenamed("sum(count)","destination_total")
//    .sort(desc("destination_total"))
//    .limit(5)
//    .show()

//   flightData2015
//    .groupBy("DEST_COUNTRY_NAME")
//    .sum("count")
//    .withColumnRenamed("sum(count)","destination_total")
//    .sort(desc("destination_total"))
//    .toDF()
//     .write
//    .format("csv")
//     .mode("overwrite")
//   //  .option("sep","\t")
//     .save("src/resources/flight-data/csv/top-destinations-2015.csv")



}
