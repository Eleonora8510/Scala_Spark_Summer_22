package com.github.Eleonora8510

import com.github.Eleonora8510.SparkUtil.{getSpark, readDataWithView}
import org.apache.spark.sql.functions.{desc, grouping_id, max, min, sum}

object Day30ExerciseJoins extends App {

  println("Exercise on Joins")

  val spark = getSpark("Sparky")

  val filePath = "src/resources/retail-data/all/online-retail-dataset.csv"
  val filePathCustomers = "src/resources/retail-data/customers.csv"

  val df = readDataWithView(spark, filePath)
  val dfCustomers = readDataWithView(spark, filePathCustomers, viewName = "dfCustomersTable")

  //TODO inner join src/resources/retail-data/all/online-retail-dataset.csv
  //TODO with src/resources/retail-data/customers.csv
  //on Customer ID in first matching Id in second

  //in other words I want to see the purchases of these customers with their full names
  //try to show it both spark API and spark SQL



  val joinExpression = dfCustomers.col("Id") === df.col("CustomerID")
  val realPurchases = dfCustomers.join(df, joinExpression)

  realPurchases.show(20)

  realPurchases.groupBy(" LastName").sum("Quantity").show()

  realPurchases.cube(" LastName", "InvoiceNo").sum("Quantity").show()
  realPurchases.cube(" LastName", "InvoiceNo")
    .agg(grouping_id(), sum("Quantity"), min("Quantity"), max("Quantity"))
    .orderBy(desc("grouping_id()")) //level 3 is everything in the table for those 2 groups
  .show()


  //in SQL
//  df.createOrReplaceTempView("dfTable")
//  dfCustomers.createOrReplaceTempView("dfCustomersTable")

    spark.sql(
    """
        |SELECT * FROM dfCustomersTable dfCT
        |JOIN dfTable dfT
        |ON dfCT.Id = dfT.CustomerID
        |WHERE ' LastName' LIKE '%Potter%'
        |ORDER BY ' LastName' DESC
        |""".stripMargin
  ).show()

  realPurchases.groupBy("StockCode")
    .pivot(" LastName")
    .sum("Quantity")
    .show(false)

  realPurchases.groupBy("StockCode", "Description")
    .pivot(" LastName")
    .sum("Quantity")
    .show(false)

}
