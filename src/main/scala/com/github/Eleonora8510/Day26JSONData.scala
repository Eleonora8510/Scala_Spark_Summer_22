package com.github.Eleonora8510

import com.github.Eleonora8510.SparkUtil.{getSpark, readDataWithView}
import org.apache.spark.sql.functions.{col, from_json, get_json_object, json_tuple, to_json}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object Day26JSONData extends App {
  println("Ch6: Working with JSON")

  val spark = getSpark("FunSpark")

  val filePath = "src/resources/retail-data/by-day/2011-08-04.csv"

  val df = readDataWithView(spark, filePath)

  val jsonDF = spark.range(1).selectExpr("""
  '{"myJSONKey" : {"myJSONValue" : [1, 2, 3]}}' as jsonString""")

  jsonDF.printSchema()
  jsonDF.show(false)


  jsonDF.select(
    get_json_object(col("jsonString"), "$.myJSONKey.myJSONValue[1]") as "column",
    json_tuple(col("jsonString"), "myJSONKey") as "jtupcol")
    .show(2, false)

  jsonDF.selectExpr(
    "json_tuple(jsonString, '$.myJSONKey.myJSONValue[1]') as column").show(2)

  val parseSchema = new StructType(Array(
    new StructField("InvoiceNo",StringType,true),
    new StructField("Description",StringType,true)))
  df.selectExpr("(InvoiceNo, Description) as myStruct")
    .select(to_json(col("myStruct")).alias("newJSON"))
    .select(from_json(col("newJSON"), parseSchema), col("newJSON"))
    .show(5, false)

  val dfParsedJSON = df.selectExpr("(InvoiceNo, Description) as myStruct")
    .withColumn("newJSON", to_json(col("myStruct")))
    .withColumn("fromJSON", from_json(col("newJSON"), parseSchema))
    .withColumn("DescFromJSON", get_json_object(col("newJSON"), "$.Description"))

  dfParsedJSON.printSchema()
  dfParsedJSON.show(5, false)

//
//  val jsPath = "src/resources/flight-data/json/2010-summary.json"
//  val jsDF = spark.read.format("txt")
//    .load(jsPath)
//
//  jsDF.printSchema()
}
