package com.github.Eleonora8510

import com.github.Eleonora8510.SparkUtil.{getSpark, readDataWithView}

object Day30ExerciseJoins extends App {

  println("Exercise on Joins")

  val spark = getSpark("Sparky")

  val filePath = "src/resources/retail-data/all/online-retail-dataset.csv"
  val filePathCustomers = "src/resources/retail-data/customers.csv"

  val df = readDataWithView(spark, filePath)
  val dfCustomers = readDataWithView(spark, filePathCustomers)

  //TODO inner join src/resources/retail-data/all/online-retail-dataset.csv
  //TODO with src/resources/retail-data/customers.csv
  //on Customer ID in first matching Id in second

  //in other words I want to see the purchases of these customers with their full names
  //try to show it both spark API and spark SQL



  val joinExpression = dfCustomers.col("Id") === df.col("CustomerID")
  dfCustomers.join(df, joinExpression)
    .show()



  //in SQL
  df.createOrReplaceTempView("dfTable")
  dfCustomers.createOrReplaceTempView("dfCustomersTable")

    spark.sql(
    """
        |SELECT * FROM dfCustomersTable dfCT
        |JOIN dfTable dfT
        |ON dfCT.Id = dfT.CustomerID
        |""".stripMargin
  ).show()

}
