package com.esri

case class Target(millis: Long, lon: Double, lat: Double) extends Ordered[Target] {

  override def compare(that: Target): Int = {
    this.millis compareTo that.millis
  }

  def distance(that: Target) = {
    Haversine.haversine(this.lat, this.lon, that.lat, that.lon)
  }

  def toCSV() = {
    f"$lon%.6f $lat%.6f"
  }

}
