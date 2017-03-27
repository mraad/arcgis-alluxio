package com.esri

/**
  */
package object mercator {

  implicit class DoubleImplicits(geo: Double) extends Serializable {

    implicit def toMercatorX() = {
      WebMercator.longitudeToX(geo)
    }

    implicit def toMercatorY() = {
      WebMercator.latitudeToY(geo)
    }
  }

}
