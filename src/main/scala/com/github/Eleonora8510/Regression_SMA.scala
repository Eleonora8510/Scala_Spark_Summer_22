package com.github.Eleonora8510

import com.github.Eleonora8510.SparkUtil.{getSpark, readDataWithView}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{MinMaxScaler, OneHotEncoder, RFormula, StandardScaler, StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, expr, lag}

object Regression_SMA extends App {
  val spark = getSpark("Trying regression")

  val filePath = "src/resources/final_project/stock_prices_.csv"
  val df = readDataWithView(spark, filePath)

  df.show(50)
  df.describe().show() //check for any inconsistencies, maybe there are some outliers
  //maybe some missing data
  println("Total number of rows: " + df.count())

  // check for existing nulls in dataframe
  val rowsWithNulls = df
    .filter(col("open").isNull
      && col("high").isNull
      && col("low").isNull
      && col("close").isNull
      && col("volume").isNull
      && col("ticker").isNull)
  println("Total number of nulls: " + rowsWithNulls.count())

  var window = Window.partitionBy("ticker").orderBy("date")
  //.rowsBetween(Window.unboundedPreceding, Window.currentRow)

  //creating new columns with previous days close prices (b_1 - close price one day ago, b_2 - close price two days ago)

  val df_withPreviousPrices = df
    .withColumn("b_1", lag(col("close"), offset = 1).over(window))
    .withColumn("b_2", lag(col("close"), offset = 2).over(window))
    .withColumn("b_3", lag(col("close"), offset = 3).over(window))
    .withColumn("b_4", lag(col("close"), offset = 4).over(window))
    .withColumn("b_5", lag(col("close"), offset = 5).over(window))
    .withColumn("b_6", lag(col("close"), offset = 6).over(window))
    .withColumn("b_7", lag(col("close"), offset = 7).over(window))
    .withColumn("b_8", lag(col("close"), offset = 8).over(window))
    .withColumn("b_9", lag(col("close"), offset = 9).over(window))
    .na.drop()
  df_withPreviousPrices.show(10)
  println("Total number of rows after dropping nulls for SMA: " + df_withPreviousPrices.count()) // the number of rows after dropping rows with nulls

  //calculating simple moving average for 3 previous days and for 9 previous days
  var df_SMA = df_withPreviousPrices
    //.select("b_1", "b_2", "b_3")
    .withColumn("SMA_3", (col("b_1") + col("b_2") + col("b_3")) / 3)
    .withColumn("SMA_9",
      (col("b_1") + col("b_2") + col("b_3") +
        col("b_4") + col("b_5") + col("b_6") +
        col("b_7") + col("b_8") + col("b_9")) / 9)

  df_SMA.show(50)

  //vectorizing close price
  val va_close = new VectorAssembler()
    .setInputCols(Array("close"))
    .setOutputCol("close vectorized")


  //scaling close price
  val ss_close = new StandardScaler()
    .setInputCol("close vectorized")
    .setOutputCol("close scaled ")

  //pipeline for close price
  val stages3 = Array(va_close, ss_close)
  val pipelineForScalingClose = new Pipeline().setStages(stages3)
  val df_scaled_close = pipelineForScalingClose.fit(df_SMA).transform(df_SMA)
  df_scaled_close.show(10, false)



  //vectorizing SMA_3 and SMA_9
  val va_SMA = new VectorAssembler()
    .setInputCols(Array("SMA_3", "SMA_9"))
    .setOutputCol("SMA vectorized")

  //scaling SMA_3 and SMA_9
  val ss_SMA = new StandardScaler()
    .setInputCol("SMA vectorized")
    .setOutputCol("SMA_scaled")

  val stages1 = Array(va_SMA, ss_SMA)
   val pipelineForScaling = new Pipeline().setStages(stages1)

  val df_scaled = pipelineForScaling.fit(df_scaled_close).transform(df_scaled_close)
  df_scaled.show(10, false)

  //indexing ticker
  val tickerIdx = new StringIndexer().setInputCol("ticker").setOutputCol("tickerInd")


  //using One Hot Encoder for categorical value tickerInd
  val ohe = new OneHotEncoder().setInputCol("tickerInd").setOutputCol("ticker_encoded")



  val stages = Array(tickerIdx, ohe)
  val pipeline = new Pipeline().setStages(stages)

  val fittedPipeline = pipeline.fit(df_scaled).transform(df_scaled)

  fittedPipeline.show(false)

// building RFormula

  val model_RFormula = new RFormula()
    .setFormula("close ~ SMA_scaled + ticker_encoded")

  val fittedRF = model_RFormula.fit(fittedPipeline)
  val preparedDF = fittedRF.transform(fittedPipeline)
  preparedDF.show(10, false)

  // building linear regression
  val lr = new LinearRegression().setMaxIter(10).setRegParam(0.3)
    .setElasticNetParam(0.8)
  println(lr.explainParams())
  val lrModel = lr.fit(preparedDF)

//  println(lrModel.predict(Vectors.dense(1000)))
//  println(lrModel.predict(Vectors.dense(-1000)))


  println(s"Intercept: ${lrModel.intercept}")
  println(s"Coefficients: ${lrModel.coefficients.toArray.mkString(",")}")

  // printing some model summary:
  val trainingSummary = lrModel.summary
  println(s"numIterations: ${trainingSummary.totalIterations}")
  println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
  trainingSummary.residuals.show()
  println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
  println(s"r2: ${trainingSummary.r2}")

  val corr = Correlation.corr(df_scaled, "SMA_scaled").head

  println(s"Pearson correlation matrix:\n $corr")


}
