package com.github.Eleonora8510

import com.github.Eleonora8510.SparkUtil.{getSpark, readDataWithView}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{array_contains, asc, col, desc, filter, size, split}

object Day26Exercise extends App {
  println("Ch6: Complex Data Types - Exercise")

  val spark = getSpark("FunSpark")

  val filePath = "src/resources/retail-data/by-day/2011-08-04.csv"

  val df = readDataWithView(spark, filePath)

  //TODO open 4th of august CSV from 2011
  //create a new dataframe with all the original columns
  //plus array of of split description
  //plus length of said array (size)
  //filter by size of at least 3
  //withSelect add 3 more columns for the first 3 words in this dataframe
  //show top 10 results sorted by first word

  //so 5 new columns (filtered rows) sorted and then top 10 results

  df
    .withColumn("Array", split(col("Description"), "  "))
    .withColumn("Length", size(split(col("Description")," ")))
    .selectExpr( "*","Array[0] as First")
    .orderBy(asc("Length"))
    .show(10, false)


  df.withColumn("Description_Array", split(col("Description"), " "))
    .withColumn("Array_Length", size(col("Description_Array")))
    // .selectExpr("Description_Array", "Array_Length", "Description_Array[0] as 1st", "Description_Array[1] as 2nd","Description_Array[2] as 3rd")
    .selectExpr("*", "Description_Array[0] as 1st", "Description_Array[1] as 2nd","Description_Array[2] as 3rd")
    .where("Array_Length >= 3")
    .orderBy(desc("1st"))
    .show(10)

  

}
