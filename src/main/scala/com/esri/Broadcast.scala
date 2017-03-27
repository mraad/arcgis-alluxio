package com.esri

import java.sql.Timestamp

case class Broadcast(
                      objectID: Long,
                      SOG: Int,
                      COG: Int,
                      heading: Int,
                      ROT: Int,
                      baseDateTime: Timestamp,
                      status: Int,
                      voyageID: String,
                      MMSI: String,
                      receiverType: String,
                      receiverID: String,
                      wkt: String
                    )