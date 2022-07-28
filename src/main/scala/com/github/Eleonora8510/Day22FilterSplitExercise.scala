package com.github.Eleonora8510

import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions.col

import scala.collection.mutable.ArrayBuffer

case class DFStats(number: Int, rowCount: Long, percentage: Long)

object Day22FilterSplitExercise extends App {

  println("Filtering and splitting DataFrame")
  val spark = SparkUtil.getSpark("Spark playground")

  //TODO open up 2014-summary.json file

  val flightPath = "src/resources/flight-data/json/2014-summary.json"

  val df = spark.read.format("json")
    .load(flightPath)

  df.show(5)
  println(s"There are ${df.count()} rows in $flightPath file")

  //TODO Task 1 - Filter only flights FROM US that happened more than 10 times
  df.where(col("ORIGIN_COUNTRY_NAME") === "United States")
    .where(col("count") > 10)
    .show()

  //TODO Task 2 - I want a random sample from all 2014 of roughly 30 percent, you can use a fixed seed
  //subtask I want to see the actual row count
  val seed = 42
  val withReplacement = false
  val fraction = 0.3
  val dfSample = df.sample(withReplacement, fraction, seed)
  println(s"Actual $fraction sample has ${dfSample.count()} rows \n")

  //TODO Task 3 - I want a split of full 2014 dataframe into 3 Dataframes with the following proportions 2,9, 5

  val dataFrames = df.randomSplit(Array(2, 9, 5), seed)

  //subtask I want to see the row count for these dataframes and percentages

  def getDataFrameStats(dFrames:Array[Dataset[Row]], df:DataFrame): ArrayBuffer[DFStats] = {
    val statsBuffer = new ArrayBuffer[DFStats]
    for ((dFrame, i) <- dFrames.zipWithIndex) {
      val percentage = dFrame.count() * 100 / df.count()
      val rowCount = dFrame.count()
      statsBuffer += DFStats(i+1, rowCount, percentage)
    }
    statsBuffer
  }

  getDataFrameStats(dataFrames, df)
    .foreach(u => println(s"DataFrame No. ${u.number}: ${u.rowCount} rows <==> ${u.percentage} % "))

}
