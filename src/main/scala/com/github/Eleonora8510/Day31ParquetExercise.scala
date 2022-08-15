package com.github.Eleonora8510

import com.github.Eleonora8510.Day31Parquet.{df, spark}
import com.github.Eleonora8510.SparkUtil.getSpark

object Day31ParquetExercise extends App {
  val spark = getSpark("Sparky")

  //TODO read parquet file from src/resources/regression
  //TODO print schema
  //TODO print a sample of some rows
  //TODO show some basic statistics - describe would be a good start
  //TODO if you encounter warning reading data THEN save into src/resources/regression_fixed

  val dfParquet = spark.read.format("parquet")
    .load("src/resources/regression")

  dfParquet.show(5)
  dfParquet.describe().show()
  dfParquet.printSchema()

}
