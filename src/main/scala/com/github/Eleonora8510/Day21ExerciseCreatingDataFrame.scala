package com.github.Eleonora8510


import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}

object Day21ExerciseCreatingDataFrame extends App {

  println("Let's create DataFrame")
  val spark = SparkUtil.getSpark("Create DataFrame")

  //TODO create 3 Rows with the following data formats, string - holding food name, int - for holding quantity, long for holding price
  //also boolean for holding isIt Vegan or not - so 4 data cells in each row
  // you will need to manually create a Schema - column names thus will be food, qty, price, isVegan
  // you  might need to import an extra type or two import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType }
  //TODO create a dataFrame called foodFrame which will hold those Rows
  //Use Select or/an SQL syntax to select and show only name and qty

  //so 3 Rows of food each Row has 4 entries (columns) so original data could be something like Chocolate, 3, 2.49, false
  //in Schema , name, qty, price are required (not nullable) while isVegan could be null

  val myFoodDataFrameSchema = new StructType(Array(
    StructField("FoodName", StringType, false),
    StructField("Quantity", IntegerType, false),
    StructField("Price", DoubleType, false),
    StructField("isVegan", BooleanType, true)))

  val myFoodRows = Seq(Row("Cepelinas", 2, 9.5, false),
    Row("Caesar salad ", 1, 7.3, false),
    Row("Pizza vegan", 1, 8.99, true)
  )

  val myRDD = spark.sparkContext.parallelize(myFoodRows)
  val foodFrame = spark.createDataFrame(myRDD, myFoodDataFrameSchema)
  foodFrame.show()

  foodFrame.createOrReplaceTempView("foodFrameTable")

  val sqlWay = spark.sql("""
      SELECT FoodName, Quantity
      FROM foodFrameTable
      """)
  sqlWay.show()

}
