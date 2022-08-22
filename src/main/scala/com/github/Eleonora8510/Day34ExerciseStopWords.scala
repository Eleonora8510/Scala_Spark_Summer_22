package com.github.Eleonora8510


import com.github.Eleonora8510.SparkUtil.getSpark
import org.apache.spark.ml.feature.{CountVectorizer, StopWordsRemover, Tokenizer}

object Day34ExerciseStopWords extends App {
  val spark = getSpark("Spark")

  val dstPath = "src/resources/Gutenberg/Alice_in_Wonderland.txt"

  val df = spark.read
    .text(dstPath)
    .toDF("text")
  df.show(5,false)



  val tkn = new Tokenizer().setInputCol("text").setOutputCol("tokenText")
  val tknColumn = tkn.transform(df.select("text"))

  val englishStopWords = StopWordsRemover.loadDefaultStopWords("english")
  val stops = new StopWordsRemover()
    .setStopWords(englishStopWords) //what we are going to remove
    .setInputCol("tokenText")
    .setOutputCol("textWithNoStopWords")
  val withoutStops = stops.transform(tknColumn)


  val cv = new CountVectorizer()
    .setInputCol("tokenText")
    .setOutputCol("countVec")
    .setVocabSize(500)
    .setMinTF(1)
    .setMinDF(3)
  val fittedCV = cv.fit(withoutStops)

  fittedCV.transform(withoutStops).show(30,false)

}
