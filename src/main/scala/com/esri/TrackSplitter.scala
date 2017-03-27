package com.esri

import scala.collection.mutable.ArrayBuffer

class TrackSplitter(time: Long, dist: Double) {

  private val _tracks = new ArrayBuffer[Seq[Target]]()
  private var _targets = new ArrayBuffer[Target]()
  private var _last = None: Option[Target]

  def +(currTarget: Target) = {
    val lastTarget = _last.getOrElse(currTarget)
    val timeDel = currTarget.millis - lastTarget.millis
    val distDel = currTarget distance lastTarget
    if (timeDel > time || distDel > dist) {
      _tracks += _targets
      _targets = new ArrayBuffer[Target]()
    }
    _targets += currTarget
    _last = Some(currTarget)
    this
  }

  def tracks(): Seq[Seq[Target]] = {
    _tracks += _targets
  }
}

object TrackSplitter {
  def apply(time: Long, dist: Double): TrackSplitter = new TrackSplitter(time, dist)
}
