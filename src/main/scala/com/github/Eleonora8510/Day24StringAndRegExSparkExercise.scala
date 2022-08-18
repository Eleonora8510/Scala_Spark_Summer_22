package com.github.Eleonora8510

import com.github.Eleonora8510.SparkUtil.{getSpark, readDataWithView}
import org.apache.spark.sql.functions.{col, count, expr, initcap, length, lpad, regexp_replace, rpad}

object Day24StringAndRegExSparkExercise extends App {
  println("Let's make some things on strings and regular expressions in Spark")

  //TODO open up March 1st, of 2011 CSV
  //Select Capitalized Description Column
  //Select Padded country column with _ on both sides with 30 characters for country name total allowed
  //ideally there would be even number of _______LATVIA__________ (30 total)
  //select Description column again with all occurences of metal or wood replaced with material
  //so this description white metal lantern -> white material lantern
  //then show top 10 results of these 3 columns

  val spark = getSpark("StringFun")

  val filePath = "src/resources/retail-data/by-day/2011-03-01.csv"

  val df = readDataWithView(spark, filePath)

  val simpleMaterials = Seq("ceramic", "ivory", "metal", "porcelain", "wood")
  val regexString = simpleMaterials.map(_.toUpperCase()).mkString("|")

  df.select(
    initcap(col("Description")).as("Capitalized Description"),
    expr("lpad(rpad(Country, 15 + int((char_length(Country))/2), '_'), 30, '_') as ___Country___"),
    regexp_replace(col("Description"), regexString, "material").alias("material_clean")
  ).show(10, false)

  spark.sql(
    """
      |SELECT
      |Description,
      |Country,
      |lpad(Country, 22, '_'),
      |rpad(Country, 22, '_'),
      |lpad(rpad(Country, 15 + (char_length(Country))/2, '_'), 30, '_') as ___Country___
      |FROM dfTable
      |""".stripMargin)
    .sample(false, fraction = 0.3)
    .show(20, false)



}
