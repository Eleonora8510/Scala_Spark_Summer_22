package com.github.Eleonora8510

object Day23LimitCollectExercise extends App {
  println("Some analysis on 1st March of 2011")
  val spark = SparkUtil.getSpark("BasicSpark")

  //TODO is load 1st of March of 2011 into dataFrame
  val filePath = "src/resources/retail-data/by-day/2011-03-01.csv"
  val df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(filePath)

  df.show(5)
  df.printSchema()
  df.createOrReplaceTempView("dfTable2011")

  //TODO get all purchases that have been made from Finland
  df.where("Country = 'Finland'").show()

  //TODO sort by Unit Price and LIMIT 20
  df
    .where("Country = 'Finland'")
    .sort("UnitPrice")
    .show() //show by default shows 20

  //TODO collect results into an Array of Rows
  //print these 20 Rows
  val arrayOfRows = df
    .where("Country = 'Finland'")
    .sort("UnitPrice")
    .limit(20)
    .collect()

  arrayOfRows.foreach(println)
  println()

  val finland2011 = spark.sql("SELECT * FROM dfTable2011 " +
    "WHERE Country = 'Finland' " +
  "ORDER BY UnitPrice DESC " +
    "LIMIT 20").collect()

  finland2011.take(5).foreach(println)

}
