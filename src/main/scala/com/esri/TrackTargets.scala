package com.esri

import com.esri.mercator._
import org.joda.time.format.DateTimeFormatter

import scala.collection.mutable.ArrayBuffer

case class TrackTargets(track: String, targets: Seq[Target]) {

  def split(time: Long, dist: Double) = {
    targets
      .foldLeft(TrackSplitter(time, dist))(_ + _)
      .tracks()
      .withFilter(_.length > 1)
      .map(TrackTargets(track, _))
  }

  def toGrid(origX: Double, origY: Double, size: Double) = {
    val lowX = origX.toMercatorX
    val lowY = origY.toMercatorY
    val grid = targets
      .map(target => {
        val q = ((target.lon.toMercatorX - lowX) / size).floor.toInt
        val r = ((target.lat.toMercatorY - lowY) / size).floor.toInt
        Cell(q, r)
      })
      .distinct
    TrackGrid(track, grid)
  }

  def smooth(alpha: Double) = {
    lowPassFilter(alpha)
  }

  case class Prev(lon: Double, lat: Double, arr: ArrayBuffer[Target])

  private def lowPassFilter(alpha: Double) = {
    // Apply low pass filter - https://en.wikipedia.org/wiki/Low-pass_filter
    val head = targets.head
    val arr = new ArrayBuffer[Target]() += head
    val prev = targets
      .tail
      .foldLeft(Prev(head.lon, head.lat, arr))((prev, target) => {
        val lon = prev.lon + alpha * (target.lon - prev.lon)
        val lat = prev.lat + alpha * (target.lat - prev.lat)
        prev.arr += Target(target.millis, lon, lat)
        Prev(lon, lat, prev.arr)
      })
    TrackTargets(track, prev.arr)
  }

  def toCSV(dateTimeFormatter: DateTimeFormatter) = {
    val wkt = targets.map(_.toCSV).mkString("LINESTRING(", ",", ")")
    val headTime = targets.head.millis
    val lastTime = targets.last.millis
    val duration = (lastTime - headTime) / 1000L
    val headDate = dateTimeFormatter.print(headTime)
    val lastDate = dateTimeFormatter.print(lastTime)
    TrackCSV(wkt, track, headDate, lastDate, duration)
  }

}
