package com.github.Eleonora8510

import com.github.Eleonora8510.SparkUtil.{getSpark, readDataWithView}
import com.github.Eleonora8510.day29WindowFunctions.{df, spark}
import org.apache.spark.sql.functions.{col, count, expr, grouping_id, sum, to_date}

object Day29Rollups extends App {
  println("Ch 7: Rollups")
  val spark = getSpark("Sparky")

  //val filePath = "src/resources/retail-data/by-day/2010-12-01.csv" //here it is a single file but wildcard should still work
  val filePath = "src/resources/retail-data/all/*.csv"
  val df = readDataWithView(spark, filePath)
  df.show(3)
  println(df.describe())


    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
  //    "MM/d/yyyy H:mm")) //legacy formatting
  val dfWithDate = df.withColumn("date", to_date(col("InvoiceDate"),
    "MM/d/yyyy H:mm")) //newer format
    .withColumn("total", expr("UnitPrice * Quantity"))
  //println(s"We have ${dfWithDate.count} rows in dfWithDate")

  val dfNoNull = dfWithDate.drop()
  //println(s"We have ${dfNoNull.count} rows in dfNoNull")
  dfNoNull.createOrReplaceTempView("dfNoNull")

  val rolledUpDF = dfNoNull.rollup("Date", "Country")
    .agg(sum("Quantity"), count("Quantity").as("countQ"), sum("total").as("totalSales"))
    .selectExpr("Date", "Country", "`sum(Quantity)` as total_quantity", "countQ", "totalSales")
    .orderBy("Date", "Country")

  rolledUpDF.show()

    //Cube
    //A cube takes the rollup to a level deeper. Rather than treating elements hierarchically, a cube
    //does the same thing across all dimensions. This means that it won’t just go by date over the
    //entire time period, but also the country. To pose this as a question again, can you make a table
    //that includes the following?
    //The total across all dates and countries
    //The total for each date across all countries
    //The total for each country on each date
    //The total for each country across all dates
    //The method call is quite similar, but instead of calling rollup, we call cube:

    // in Scala
    dfNoNull.cube("Date", "Country").agg(grouping_id(),sum(col("Quantity")), sum("total"))
      .select("Date", "Country", "grouping_id()", "sum(Quantity)", "sum(total)").orderBy("Date")
      .show(50)

        //Grouping Metadata
        //Sometimes when using cubes and rollups, you want to be able to query the aggregation levels so
        //that you can easily filter them down accordingly. We can do this by using the grouping_id,
        //which gives us a column specifying the level of aggregation that we have in our result set. The
        //query in the example that follows returns four distinct grouping IDs:
        //Table 7-1. Purpose of grouping IDs
        //Grouping
        //ID
        //Description
        //3
        //This will appear for the highest-level aggregation, which will gives us the total quantity
        //regardless of customerId and stockCode.
        //2
        //This will appear for all aggregations of individual stock codes. This gives us the total quantity
        //per stock code, regardless of customer.
        //1
        //This will give us the total quantity on a per-customer basis, regardless of item purchased.
        //0
        //This will give us the total quantity for individual customerId and stockCode combinations.

        dfNoNull.cube("customerId", "stockCode")
          .agg(grouping_id(), sum("Quantity"), sum("total"))
      .orderBy(expr("grouping_id()").desc)
      .show()

  //Pivot
  //Pivots make it possible for you to convert a row into a column. For example, in our current data
  //we have a Country column. With a pivot, we can aggregate according to some function for each
  //of those given countries and display them in an easy-to-query way:
  dfWithDate.show(5, false)
  dfWithDate
    .select("Country")
    .distinct()
    .orderBy("Country")
    .show(30)

  val pivoted = dfWithDate.groupBy("date").pivot("Country").sum()



  pivoted.show(20, false)
  //many columns equal to distinct country count * numeric columns in original dfWithDate

  val pivotedSelect = dfWithDate.groupBy("date").pivot("Country").agg("Quantity"->"avg")


    pivotedSelect.show()
}
