package com.github.Eleonora8510

import com.github.Eleonora8510.SparkUtil.{getSpark, readDataWithView}
import org.apache.spark.sql.functions.{col, expr, initcap, lit, lower, lpad, ltrim, regexp_extract, regexp_replace, rpad, rtrim, translate, trim, upper}

object Day24SparkStrings extends App {

  println("CH6: Working with Strings")
  val spark = getSpark("StringFun")

  val filePath = "src/resources/retail-data/by-day/2010-12-01.csv"

  val df = readDataWithView(spark, filePath)

  //Working with Strings
  //String manipulation shows up in nearly every data flow, and it’s worth explaining what you can
  //do with strings. You might be manipulating log files performing regular expression extraction or
  //substitution, or checking for simple string existence, or making all strings uppercase or
  //lowercase.
  //Let’s begin with the last task because it’s the most straightforward. The initcap function will
  //capitalize every word in a given string when that word is separated from another by a space

  df.select(col("Description"),
    initcap(col("Description"))).show(3, false)

  //all SQL functions are listed here; https://spark.apache.org/docs/latest/api/sql/index.html
  spark.sql("SELECT Description, initcap(Description) FROM dfTable")
    .show(3, false) //false shows full strings in columns, without truncation/cutting

  //As just mentioned, you can cast strings in uppercase and lowercase, as well:

  df.select(col("Description"),
    lower(col("Description")),
    upper(lower(col("Description")))) //the lower here is not necessary of course example shows nesting
    .show(3, false)

  spark.sql("SELECT Description, lower(Description), " +
    "upper(lower(Description)) FROM dfTable") //again upper(lower is not needed but it might be useful for some other function
    .show(3, false)

  //Another trivial task is adding or removing spaces around a string. You can do this by using lpad,
  //ltrim, rpad and rtrim, trim:

  df.select(
    col("CustomerId"), //not needed just to show you we are working with the dataframe
    ltrim(lit(" HELLO ")).as("ltrim"),
    rtrim(lit(" HELLO ")).as("rtrim"),
    trim(lit(" HELLO ")).as("trim"),
    lpad(lit("HELLO"), 3, " ").as("lp"),
    rpad(lit("HELLO"), 10, " ").as("rp"),
    //😁 is represented by 4 bytes that's why you have \u twice
    lpad(rpad(lit("HELLO"), 10, "*"), 15, "\uD83D\uDE01").as("pad15charstotal")
  ).show(2)
  //so pad even works with high value unicode after 128k which is smileys

  //so lpad (rpad is similar)
  //Left-pad the string column with pad to a length of len.
  // If the string column is longer than len, the return value is shortened to len characters.

  spark.sql(
    """
      |SELECT
      | CustomerId,
      |ltrim(' HELLLOOOO ') as ltrim,
      |rtrim(' HELLLOOOO '),
      |trim(' HELLLOOOO '),
      |lpad('HELLOOOO ', 3, ' '),
      |rpad('HELLOOOO ', 10, ' ')
      | FROM dfTable
      |""".stripMargin)
    .show(2)


  //Regular Expressions
  //Probably one of the most frequently performed tasks is searching for the existence of one string
  //in another or replacing all mentions of a string with another value. This is often done with a tool
  //called regular expressions that exists in many programming languages. Regular expressions give
  //the user an ability to specify a set of rules to use to either extract values from a string or replace
  //them with some other values.
  //Spark takes advantage of the complete power of Java regular expressions. The Java regular
  //expression syntax departs slightly from other programming languages, so it is worth reviewing
  //before putting anything into production. There are two key functions in Spark that you’ll need in
  //order to perform regular expression tasks: regexp_extract and regexp_replace. These
  //functions extract values and replace values, respectively.
  //Let’s explore how to use the regexp_replace function to replace substitute color names in our
  //description column:

  //prepping regex in Scala - could do it by hand of course by writing up full regex
  val simpleColors = Seq("black", "white", "red", "green", "blue")
  val regexString = simpleColors.map(_.toUpperCase).mkString("|")
  println(regexString) //"BLACK|WHITE|RED|GREEN|BLUE"

  df.select(
    regexp_replace(col("Description"), regexString, "COLOR").alias("color_clean"),
    col("Description"))
    .show(5, false)

  spark.sql(
    """
      |SELECT
      |regexp_replace(Description, 'BLACK|WHITE|RED|GREEN|BLUE', 'colorful') as
      |color_clean, Description
      |FROM dfTable
      |""".stripMargin)
    .show(5, false)

  df.select(translate(col("Description"), "LEIT", "1317"), col("Description"))
    .show(2)

  //so the idea is to extract some value out of particular row and basically have it in

  df.select(
    regexp_extract(col("Description"), regexString, 0).alias("color_clean"),
    col("Description"))
    .where("CHAR_LENGTH(color_clean)>0").show(15)

  //Sometimes, rather than extracting values, we simply want to check for their existence. We can do
  //this with the contains method on each column. This will return a Boolean declaring whether the
  //value you specify is in the column’s string

  //so we add a new column(with boolean whether there is black or white in description)
  //then filter by that column
  val containsBlack = col("Description").contains("BLACK")
  val containsWhite = col("DESCRIPTION").contains("WHITE")
  df.withColumn("hasSimpleColor", containsBlack.or(containsWhite))
    .where("hasSimpleColor")
    .select("Description").show(3, false)

  //SQL with instr function
  spark.sql(
    """
      |SELECT Description FROM dfTable
      |WHERE instr(Description, 'BLACK') >= 1 OR instr(Description, 'WHITE') >= 1
      |""".stripMargin)
    .show(5, false)

  //This is trivial with just two values, but it becomes more complicated when there are values.
  //Let’s work through this in a more rigorous way and take advantage of Spark’s ability to accept a
  //dynamic number of arguments. When we convert a list of values into a set of arguments and pass
  //them into a function, we use a language feature called varargs. Using this feature, we can
  //effectively unravel an array of arbitrary length and pass it as arguments to a function. This,
  //coupled with select makes it possible for us to create arbitrary numbers of columns
  //dynamically:
  val multipleColors = Seq("black", "white", "red", "green", "blue")
  val selectedColumns = multipleColors.map(color => {
    col("Description").contains(color.toUpperCase).alias(s"is_$color")
  }):+expr("*") // could also append this value

  df.select(selectedColumns:_*)
    //we unroll our sequence of Columsn into multiple individual arguments
    //because select takes multiple columns one by one NOT an Sequence of columns
    .show(10, false)

  //so I do not have to give all selected columns
  df.select(selectedColumns.head, selectedColumns(3), selectedColumns.last, col("Description"))
    .show(5, false)

  df.select(selectedColumns:_*).where(col("is_white").or(col("is_red")))
    .select("Description").show(3, false)

//  val numbers = Array(1,5,6,20,5)
//  println(numbers:_*) //unrolls a sequence (Array here) will print a tuple of numbers since it is equivalent to
//  println(1,5,6,20,5)
  //so check if your method or function has * at the end of some parameter then you can unroll some sequence into those parameters
}