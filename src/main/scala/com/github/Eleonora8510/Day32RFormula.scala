package com.github.Eleonora8510

import com.github.Eleonora8510.SparkUtil.{getSpark, readDataWithView}
import org.apache.spark.ml.feature.RFormula

object Day32RFormula extends App {
  println("CH25: RFormula")
  //https://spark.apache.org/docs/latest/ml-features.html#rformula

  val spark = getSpark("sparky")
  spark.sparkContext.setLogLevel("WARN")

  val dataset = spark.createDataFrame(Seq(
    (7, "US", 18, 1.0),
    (8, "CA", 12, 0.0),
    (9, "NZ", 15, 0.0),
    (10, "LV", 215, 0.0),
    (15, "LT", 515, 55.0),
  )).toDF("id", "country", "hour", "clicked")

  dataset.show()

  val formula = new RFormula()
    //we are saying we want the label be from clicked column
    //and country and hour columns to be used for features
    .setFormula("clicked ~ country + hour")
    .setFeaturesCol("MYfeatures") //default is features which is fine
    .setLabelCol("MYlabel") //default is label which is usually fine

  val output = formula.fit(dataset).transform(dataset)
    //    .select("features", "label")
    .show()

  //TODO load into dataframe from retail-data by-day December 1st
  val dfDec1 = readDataWithView(spark, "src/resources/retail-data/by-day/2010-12-01.csv")
  dfDec1.show(5)

  //TODO create RFormula to use Country as label and only UnitPrice and Quantity as Features
  val formulaDec1 = new RFormula()
    .setFormula("Country ~ UnitPrice + Quantity")
    .setFeaturesCol("Features")
    //.setLabelCol("Label")

  //TODO make sure they are numeric columns - we do not want one hot encoding here
  //you can leave column names at default

  //create output dataframe with the the formula performing fit and transform

  val outputDec1 = formulaDec1.fit(dfDec1).transform(dfDec1)
    .select( "Country", "UnitPrice", "Quantity","Features", "label")
    .show()

  //TODO BONUS try creating features from ALL columns in the Dec1st CSV except of course Country (using . syntax)
  //This should generate very sparse column of features because of one hot encoding
  val formulaFull = new RFormula()
    .setFormula("Country ~ .") //this will create one hot encoding for all string columns
    //.setFeaturesCol("MyFeatures")
    //.setLabelCol("MyLabel") //now default Label "label"

  val outputFull = formulaFull.fit(dfDec1).transform(dfDec1)

  outputFull
    .show(false)

}
