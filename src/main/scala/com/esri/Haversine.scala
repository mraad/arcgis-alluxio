package com.esri

import scala.math._

/**
  * https://en.wikipedia.org/wiki/Haversine_formula
  * https://rosettacode.org/wiki/Haversine_formula#Scala
  * https://en.wikipedia.org/wiki/Earth_radius
  */

object Haversine {
  val R = 6371008.8 //radius in m

  def haversine(lat1: Double, lon1: Double, lat2: Double, lon2: Double) = {
    val dLat = (lat2 - lat1).toRadians
    val dLon = (lon2 - lon1).toRadians

    val sinLat = sin(dLat * 0.5)
    val sinLon = sin(dLon * 0.5)
    val a = sinLat * sinLat + sinLon * sinLon * cos(lat1.toRadians) * cos(lat2.toRadians)
    val c = 2.0 * asin(sqrt(a))
    R * c
  }

  def main(args: Array[String]): Unit = {
    println(haversine(36.12, -86.67, 33.94, -118.40)) // 2886448.4297648543
  }

}
