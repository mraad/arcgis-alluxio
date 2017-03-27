package com.esri

import com.vividsolutions.jts.geom.{GeometryFactory, PrecisionModel}
import com.vividsolutions.jts.io.WKTReader
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.util.ParseModes
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.joda.time.format.DateTimeFormat

object TrackAssembler extends App {
  val spark = SparkSession
    .builder()
    .appName("Track Assembler")
    .getOrCreate()

  import spark.implicits._

  try {
    val conf = spark.sparkContext.getConf

    val schema = ScalaReflection.schemaFor[Broadcast].dataType.asInstanceOf[StructType]

    // Time between targets to be in the same track
    val time = conf.getLong("spark.app.time.millis", 60 * 60 * 1000L)
    // Distance between targets to be in the same track
    val dist = conf.getDouble("spark.app.dist.meters", 3000.0)
    // LowPass filter factor
    val alpha = conf.getDouble("spark.app.alpha", 0.85)

    spark
      .read
      .format("csv")
      .option("delimiter", conf.get("spark.app.input.delimiter", "\t"))
      .option("header", conf.getBoolean("spark.app.input.header", false))
      .option("mode", ParseModes.DROP_MALFORMED_MODE)
      .option("timestampFormat", conf.get("spark.app.input.timestampFormat", "yyyy-MM-dd HH:mm:ss"))
      .schema(schema)
      .load(conf.get("spark.app.input.path", "alluxio://localhost:19998/Broadcast.csv"))
      .as[Broadcast]
      // TODO: Apply dynamic filtering using quasi-string
      // Status 0 indicates "under way using engine"
      .filter(_.status == 0)
      .mapPartitions(iter => {
        val reader = new WKTReader(new GeometryFactory(new PrecisionModel(1000000.0)))
        iter.map(line => {
          val geom = reader.read(line.wkt)
          val coord = geom.getCoordinate
          TrackTarget(line.voyageID, Target(line.baseDateTime.getTime, coord.x, coord.y))
        })
      })
      // TODO: Apply Geospatial filter.
      .sort("track", "target")
      .groupBy("track")
      .agg(collect_list("target") as "targets")
      .as[TrackTargets]
      .flatMap(_.split(time, dist))
      // TODO: Flag anomalous tracks
      .map(_.smooth(alpha))
      .mapPartitions(iter => {
        val dateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZoneUTC()
        iter.map(_.toCSV(dateTimeFormatter))
      })
      .repartition(conf.getInt("spark.app.output.repartition", 8))
      .write
      .option("delimiter", conf.get("spark.app.output.delimiter", "\t"))
      .option("header", "false")
      .mode(SaveMode.Overwrite)
      .csv(conf.get("spark.app.output.path", "alluxio://localhost:19998/tracks"))

  } finally {
    spark.stop()
  }

}
