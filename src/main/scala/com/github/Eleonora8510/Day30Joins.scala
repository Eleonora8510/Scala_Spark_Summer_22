package com.github.Eleonora8510

import com.github.Eleonora8510.SparkUtil.getSpark
import org.apache.spark.sql.functions.expr

object Day30Joins extends App {

  println("Ch8: Joins")
  val spark = getSpark("Sparky")

  //Join Expressions
  //A join brings together two sets of data, the left and the right, by comparing the value of one or
  //more keys of the left and right and evaluating the result of a join expression that determines
  //whether Spark should bring together the left set of data with the right set of data. The most
  //common join expression, an equi-join, compares whether the specified keys in your left and
  //right datasets are equal. If they are equal, Spark will combine the left and right datasets. The
  //opposite is true for keys that do not match; Spark discards the rows that do not have matching
  //keys. Spark also allows for much more sophsticated join policies in addition to equi-joins. We
  //can even use complex types and perform something like checking whether a key exists within an
  //array when you perform a join.

  //Join Types
  //Whereas the join expression determines whether two rows should join, the join type determines
  //what should be in the result set. There are a variety of different join types available in Spark for
  //you to use:
  //Inner joins (keep rows with keys that exist in the left and right datasets)
  //Outer joins (keep rows with keys in either the left or right datasets)
  //Left outer joins (keep rows with keys in the left dataset)
  //Right outer joins (keep rows with keys in the right dataset)
  //Left semi joins (keep the rows in the left, and only the left, dataset where the key
  //appears in the right dataset)
  //Left anti joins (keep the rows in the left, and only the left, dataset where they do not
  //appear in the right dataset)
  //Natural joins (perform a join by implicitly matching the columns between the two
  //datasets with the same names)
  //Cross (or Cartesian) joins (match every row in the left dataset with every row in the
  //right dataset)
  //If you have ever interacted with a relational database system, or even an Excel spreadsheet, the
  //concept of joining different datasets together should not be too abstract. Let’s move on to
  //showing examples of each join type. This will make it easy to understand exactly how you can
  //apply these to your own problems. To do this, let’s create some simple datasets that we can use
  //in our examples
  //some simple Datasets

  import spark.implicits._ //implicits will let us use toDF on simple sequence
  //regular Sequence does not have toDF method that why we had to use implicits from spark
  val person = Seq(
    (0, "Bill Chambers", 0, Seq(100)),
    (1, "Matei Zaharia", 1, Seq(500, 250, 100)),
    (2, "Michael Armbrust", 1, Seq(250, 100)),
    (3, "Eleonora Sturo-Borovaja", 2, Seq(150, 100)),
    (4, "Victoria Beckman", 42, Seq(100,250)))
    .toDF("id", "name", "graduate_program", "spark_status")
  val graduateProgram = Seq(
    (0, "Masters", "School of Information", "UC Berkeley"),
    (2, "Masters", "EECS", "UC Berkeley"),
    (1, "Ph.D.", "EECS", "UC Berkeley"),
    (3, "Masters", "MIF", "VU"),
    (4, "Masters", "EECS", "University of Latvia"))
    .toDF("id", "degree", "department", "school")
  val sparkStatus = Seq(
    (500, "Vice President"),
    (250, "PMC Member"),
    (100, "Contributor"),
    (150, "Student"))
    .toDF("id", "status")

  person.show()
  graduateProgram.show()
  sparkStatus.show()

  person.createOrReplaceTempView("person")
  graduateProgram.createOrReplaceTempView("graduateProgram")
  sparkStatus.createOrReplaceTempView("sparkStatus")

  //Inner Joins
  //Inner joins evaluate the keys in both of the DataFrames or tables and include (and join together)
  //only the rows that evaluate to true. In the following example, we join the graduateProgram
  //DataFrame with the person DataFrame to create a new DataFrame:

  // in Scala - notice the triple ===
  val joinExpression = person.col("graduate_program") === graduateProgram.col("id")


  //Inner joins are the default join, so we just need to specify our left DataFrame and join the right in
  //the JOIN expression:
  person.join(graduateProgram, joinExpression, "inner").show()

  //same in spark sql
  spark.sql(
    """
      |SELECT * FROM person JOIN graduateProgram
      |ON person.graduate_program = graduateProgram.id
      |""".stripMargin)
    .show()

  //Outer joins
//Outer joins evaluate the keys in both of the DataFrames or tables and includes (and
  //joins together) the rows that evaluate to true or false.
  // If there is no equivalent row in either the left or right DataFrame, Spark will insert null:

 //you can specify
  val joinType = "outer"
  person.join(graduateProgram, joinExpression, joinType).show()

  //in SQL
spark.sql(
  """
    |  SELECT * FROM person FULL OUTER JOIN graduateProgram
    |    ON person.graduate_program = graduateProgram.id
    |""".stripMargin
).show()

  //Left Outer Joins
  //Left outer joins evaluate the keys in both of the DataFrames or tables and includes
  // all rows from the left DataFrame as well as any rows
  // in the right DataFrame that have a match in the left DataFrame.
  // If there is no equivalent row in the right DataFrame, Spark will insert null:

  println("LEFT OUTER JOIN")

  graduateProgram.join(person, joinExpression, "left_outer").show()

  //in SQL
  spark.sql(
    """
      |SELECT * FROM person LEFT OUTER JOIN graduateProgram
      |    ON person.graduate_program = graduateProgram.id
      |    """.stripMargin
  ).show()


  println("RIGHT OUTER JOIN")

  graduateProgram.join(person, joinExpression, "right_outer").show()

  //in SQL
  spark.sql(
    """
      |SELECT * FROM person RIGHT OUTER JOIN graduateProgram
      |    ON person.graduate_program = graduateProgram.id
      |    """.stripMargin
  ).show()

  println("LEFT SEMI JOIN")

  graduateProgram.join(person, joinExpression, "left_semi").show() // show only programs with graduates

  person.join(graduateProgram, joinExpression, "left_semi").show() // show only persons who attended a known university

  // we create a new DAtaFrame by using Union of two dataframes
  val gradProgram2 = graduateProgram.union(Seq(
    (0, "Masters", "Duplicated Row", "Duplicated School")).toDF())
  gradProgram2.createOrReplaceTempView("gradProgram2")
  gradProgram2.join(person, joinExpression, "left_semi").show()

  //SQL
  spark.sql(
    """
      |SELECT * FROM gradProgram2 LEFT SEMI JOIN person
      |  ON gradProgram2.id = person.graduate_program
      |""".stripMargin
  ).show()

  graduateProgram.join(person, joinExpression, "left_anti").show() // should show only programs without graduates

  spark.sql(
    """
      |SELECT * FROM graduateProgram LEFT ANTI JOIN person
      |  ON graduateProgram.id = person.graduate_program
      |""".stripMargin)
      .show()

  //natural joins
  spark.sql(
    """
      |SELECT * FROM graduateProgram NATURAL JOIN person
      |""".stripMargin
  ).show() // will produce logically incorrect joins because id columns have different meaning in these tables

  //so we have 6 persons
  //and 5 graduate programs
  // so we expect 5*6 = 30 rows in a cross (Cartesian) join

  println("CROSS JOIN")

  spark.conf.set("spark.sql.crossJoin.enable", true) //TODO see if it is correct
  // graduateProgram.join(person, joinExpression, "cross").show(32)

  graduateProgram.crossJoin(person).show(32)


  spark.sql(
    """
      |SELECT * FROM graduateProgram
      |CROSS JOIN person
      |""".stripMargin
  ).show(32)


// JOINS on complex types
  person.withColumnRenamed("id", "personId") //we rename personID to avoid confusion with spark status id
    .join(sparkStatus, expr("array_contains(spark_status, id)")).show()

spark.sql(
  """
    |SELECT * FROM
    |  (select id as personId, name, graduate_program, spark_status FROM person)
    |  INNER JOIN sparkStatus ON array_contains(spark_status, id)
    |""".stripMargin
).show()
}
