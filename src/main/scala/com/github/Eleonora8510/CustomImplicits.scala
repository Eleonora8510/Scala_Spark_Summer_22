package com.github.Eleonora8510

import org.apache.spark.sql.{Encoder, Encoders}

object CustomImplicits extends App {
  implicit val flightEncoder: Encoder[Flight] = Encoders.product[Flight]

}
