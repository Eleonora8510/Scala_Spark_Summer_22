package com.github.Eleonora8510

import com.github.Eleonora8510.CustomImplicits.flightEncoder
import org.apache.spark.sql.SparkSession

case class Flight(DEST_COUNTRY_NAME: String,
                  ORIGIN_COUNTRY_NAME: String,
                  count: BigInt)

object Type_safe_parquet extends App {
  println("Testing parquet data type")

  val spark = SparkSession.builder().appName("test").master("local").getOrCreate()

  val flightsDF = spark.read
    .parquet("src/resources/flight-data/parquet/2010-summary.parquet")

  val flights = flightsDF.as[Flight]
//  flights
//    .filter(filter_row => filter_row.ORIGIN_COUNTRY_NAME != "Canada")
//    .map(flight_row => flight_row)
//    .take(5)

  flights
    .take(5)
    .filter(flight_row => flight_row.ORIGIN_COUNTRY_NAME != "Canada")
    .map(fr => Flight(fr.DEST_COUNTRY_NAME, fr.ORIGIN_COUNTRY_NAME, fr.count + 5))

}
