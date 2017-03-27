package com.esri

import java.sql.Timestamp

case class BroadcastRow(
                         objectID: Long,
                         heading: Int,
                         baseDateTime: Timestamp,
                         status: Int,
                         voyageID: String,
                         MMSI: String,
                         lon: Double,
                         lat: Double
                       )

object BroadcastRow {
  def apply(
             broadcast: Broadcast,
             lon: Double,
             lat: Double
           ): BroadcastRow = new BroadcastRow(
    broadcast.objectID,
    broadcast.heading,
    broadcast.baseDateTime,
    broadcast.status,
    broadcast.voyageID,
    broadcast.MMSI,
    lon, lat)
}