package com.github.Eleonora8510

import com.github.Eleonora8510.SparkUtil.getSpark
import org.apache.spark.ml.feature.{StandardScaler, Tokenizer, VectorAssembler}
import org.apache.spark.sql.functions.{col, concat_ws, expr}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import java.io.FileWriter
import scala.io.Source

object Day33Exercise extends App {

  //TODO Read text from url
  //https://www.gutenberg.org/files/11/11-0.txt - Alice in Wonderland
  //https://stackoverflow.com/questions/44961433/process-csv-from-rest-api-into-spark
  //above link shows how to read csv file from url
  //you can adopt it to read a simple text file directly as well

  //alternative download and read from file locally

  val spark = getSpark("Sparky")

  val url = "https://www.gutenberg.org/files/11/11-0.txt"
  val dstPath = "src/resources/Gutenberg/Alice_in_Wonderland.txt"

  val bufferedSource = Source.fromURL(url)
  val lines = bufferedSource.mkString
  bufferedSource.close()
  println(lines.length)

  val fw = new FileWriter(dstPath)
  fw.write(lines)
  fw.close()

  //TODO create a DataFrame with a single column called text which contains above book line by line

  val df = spark.read
    .text(dstPath)
    .toDF("text")
  df.show(5,false)

  //TODO create new column called words with will contain Tokenized words of text column
  val tkn = new Tokenizer().setInputCol("text").setOutputCol("tokenText")
  val tknColumn = tkn.transform(df.select("text"))
    tknColumn.show(10, false)

  //TODO create column called textLen which will be a character count in text column
  //https://spark.apache.org/docs/2.3.0/api/sql/index.html#length can use or also length function from spark

  //TODO create column wordCount which will be a count of words in words column
  //can use count or length - words column will be Array type

  val dfWithStats = tknColumn
    .withColumn("textLen", expr("LENGTH(text)"))
    .withColumn("wordCount", expr("size(split(text, ' '))"))
    dfWithStats.show(10)

  //TODO create Vector Assembler which will transform textLen and wordCount into a column called features
  //features column will have a Vector with two of those values

  val va = new VectorAssembler()
    .setInputCols(Array("textLen", "wordCount"))
    .setOutputCol("features") //otherwise we get a long hash type column name
  val dfAssembled = va.transform(dfWithStats)
  dfAssembled.show(10, false)

  //TODO create StandardScaler which will take features column and output column called scaledFeatures
  //it should be using mean and variance (so both true)

  val ss = new StandardScaler()
    .setInputCol("features")
    .setOutputCol("scaledFeatures")
    .setWithStd(true)
    .setWithMean(true )

  val dfScaled = ss.fit(dfAssembled).transform(dfAssembled)
  dfScaled.show(5, false)

  //TODO create a dataframe with all these columns - save to alice.csv file with all columns

  dfScaled.createOrReplaceTempView("dfTable")

  //casting to string
  val castedDF = spark.sql(
    """
      |SELECT text,
      |CAST(tokenText AS String),
      |textLen,
      |wordCount,
      |CAST(features AS String),
      |CAST(scaledFeatures AS String)
      |FROM dfTable
      |""".stripMargin
  )

  castedDF.printSchema()

  //writing to .csv file
  castedDF
    .write
      .format("csv")
      .option("header", "true")
      .mode("overwrite")
      .save("src/resources/Gutenberg/alice.csv")

}
