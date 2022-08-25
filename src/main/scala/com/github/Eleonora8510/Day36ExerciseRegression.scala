package com.github.Eleonora8510

import com.github.Eleonora8510.SparkUtil.getSpark
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.LinearRegression

object Day36ExerciseRegression extends  App{
  val spark = getSpark("Sparky")

  //TODO open "src/resources/csv/range3d"

  //TODO Transform x1,x2,x3 into features(you cna use VectorAssembler or RFormula), y can stay, or you can use label/value column

  //TODO create  a Linear Regression model, fit it to our 3d data

  //TODO print out intercept
  //TODO print out all 3 coefficients

  //TODO make a prediction if values or x1, x2 and x3 are respectively 100, 50, 1000

  val src = "src/resources/csv/range3d"
  val df =spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .load(src)

  df.printSchema()

  //renaming for label
  val dfWithLabel = df.withColumnRenamed("y", "label")
  dfWithLabel.show(5, false)

  //vectorizing features
    val va_features = new VectorAssembler()
      .setInputCols(Array("x1", "x2", "x3"))
      .setOutputCol("features")

  val df_prepared = va_features.transform(dfWithLabel)
  df_prepared.show(5, false)

  val lr = new LinearRegression().setLabelCol("label").setFeaturesCol("features")

  val lrModel = lr.fit(df_prepared)

  println(s"Intercept: ${lrModel.intercept}")
  println(s"Coefficients: ${lrModel.coefficients.toArray.mkString(",")}")

  println("Prediction for values (100, 50, 1000) is:")
  println(lrModel.predict(Vectors.dense(100,50, 1000)))




}
