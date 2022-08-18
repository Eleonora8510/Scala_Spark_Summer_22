package com.github.Eleonora8510

import com.github.Eleonora8510.SparkUtil.{getSpark, readDataWithView}
import org.apache.spark.sql.functions.{coalesce, col, expr, lit, months_between, to_date, to_timestamp}

object Day25DatesNulls extends App {
  println("Ch6: Dealing with null data")
  val spark = getSpark("StringFun")

  val filePath = "src/resources/retail-data/by-day/2010-12-01.csv"

  val df = readDataWithView(spark, filePath)

  df.select(
    to_date(lit("2016-01-01")).alias("start"),
    to_date(lit("2017-05-22")).alias("end"))
    .select(months_between(col("start"), col("end"))).show(1)

  val dateFormat = "yyyy-dd-MM"
  val euroFormat = "dd-MM-yy"
  val cleanDateDF = spark.range(1).select(
    to_date(lit("2017-12-11"), dateFormat).alias("date"),
    to_date(lit("2017-20-12"), dateFormat).alias("date2"),
    to_date(lit("02-08-22"),euroFormat).alias("date3"),
    to_date(lit("22_02_08"), "yy_dd_MM").alias("date4")
  )
  cleanDateDF.createOrReplaceTempView("dateTable2")



  cleanDateDF.show(3, false)

//  spark.sql(
//    """SELECT to_date(date, 'yyyy-dd-MM'), to_date(date2, 'yyyy-dd-MM'), to_date(date)""".stripMargin)
  //cleanDateDF.select(to_timestamp(col("date"), dateFormat)).show()

  df
    .withColumn("mynulls", expr("null"))
    .select(coalesce(col("Description"), col("CustomerId")))
    .show()

  spark.sql(
    """ SELECT
      |  ifnull(null, 'return_value'),
      |  nullif('value', 'value'),
      |  nvl(null, 'return_value'),
      |  nvl2('not_null', 'return_value', "else_value")
      |FROM dfTable LIMIT 1""".stripMargin
  ).show(2)

  //drop
  df.na.drop()
  df.na.drop("any")

  df.na.drop("all")
  println(df.na.drop("all", Seq("StockCode", "InvoiceNo")).count())

  //fill
  df.na.fill(777, Seq("StockCode", "InvoiceNo", "CustomerID"))
    .where(expr("CustomerID = 777"))
    .show(10, false)

  val fillColValues = Map("CustomerID" -> 5, "Description" -> "No Description")
  df.na.fill(fillColValues)
    .where(expr("Description = 'No Description'"))
    .show(5, false)

  df.na.replace("Description", Map("VINTAGE SNAP CARDS" -> "BRAND NEW CARDS"))
    .where("Description = 'BRAND NEW CARDS'")
    .show(5,false)
}
