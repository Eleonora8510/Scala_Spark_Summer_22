package com.github.Eleonora8510

import org.apache.spark.sql.SparkSession

object Day20ExerciseStructuredAPI extends App {

  println(s"Scala version: ${util.Properties.versionNumberString}")

  val spark = SparkSession.builder().appName("tourDeSpark").master("local").getOrCreate()
  spark.conf.set("spark.sql.shuffle.partitions", "5")
  println(s"Session started on Spark version ${spark.version}")

  //TODO create a DataFrame with a single column called JulyNumbers from 1 to 31
  //TODO Show all 31 numbers

  val range1To31 = spark.range(1,32).toDF("JulyNumbers")
  range1To31.show(31)

  //TODO Create another dataframe with numbers from 100 to 3100
  //TODO show last 5 numbers

  val bigRange = spark.range(100, 3101).toDF
  bigRange.tail(5).foreach(println)

  //another approach
  val range100To3100 = spark.range(100, 3101).toDF.collect()
  range100To3100.slice(2996, 3001).foreach(println)

}

