package com.github.Eleonora8510

import com.github.Eleonora8510.SparkUtil.{getSpark, readCSVWithView}
import org.apache.spark.sql.functions.{col, current_date, current_timestamp, datediff, months_between}

object Day25DateTimestampExercise extends App {
println("Let's create some new columns with dates and timestamps in Spark")

  val spark = getSpark("StringFun")

  val filePath = "src/resources/retail-data/by-day/2011-03-01.csv"

  val df = readCSVWithView(spark, filePath)

  //TODO open March 1st , 2011
  //Add new column with current date
  //Add new column with current timestamp
  //add new column which contains days passed since InvoiceDate (here it is March 1, 2011 but it could vary)
  //add new column with months passed since InvoiceDate

  df.withColumn("Current date", current_date())
    .withColumn("Current time", current_timestamp())
    .withColumn("Day difference", datediff(col("Current date"), col("InvoiceDate")))
    .withColumn("Month difference", months_between(col("Current date"), col("InvoiceDate")))
    .show(2, false)

  //in SQL
  spark.sql(
    """
      |SELECT *,
      |current_date,
      |current_timestamp,
      |datediff('2022-08-02', '2011-03-01') as day_difference,
      |months_between('2022-08-02', '2011-03-01') as month_difference
      |FROM dfTable
      |""".stripMargin)
    .show(3, false)

}
