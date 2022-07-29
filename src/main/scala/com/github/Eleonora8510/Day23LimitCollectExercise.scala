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

  //TODO get all purchases that have been made from Finland
  df.where("Country = 'Finland'").show()

  //TODO sort by Unit Price and LIMIT 20
  df.sort("UnitPrice")
    .where("Country = 'Finland'")
    .show() //show by default shows 20

  //TODO collect results into an Array of Rows
  //print these 20 Rows
  val arrayOfRows = df.sort("UnitPrice")
    .where("Country = 'Finland'")
    .limit(20)
    .collect()

  arrayOfRows.foreach(println)




  //You can use either SQL or Spark or mix of syntax

}
