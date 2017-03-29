package com.esri

import com.vividsolutions.jts.geom.{GeometryFactory, PrecisionModel}
import com.vividsolutions.jts.io.WKTReader
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.util.ParseModes
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SaveMode, SparkSession}

object PathFinder extends App {
  val spark = SparkSession
    .builder()
    .appName("Path Finder")
    .getOrCreate()

  import spark.implicits._

  try {
    val conf = spark.sparkContext.getConf

    val schema = ScalaReflection.schemaFor[Broadcast].dataType.asInstanceOf[StructType]

    val time = conf.getLong("spark.app.time.millis", 60 * 60 * 1000L)
    val dist = conf.getDouble("spark.app.dist.meters", 3000.0)
    val origX = conf.getDouble("spark.app.orig.x", -81.0)
    val origY = conf.getDouble("spark.app.orig.y", 25.0)
    val cellSize = conf.getDouble("spark.app.cell.size", 1000.0)

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
      // Status 0 is "under way using engine"
      .filter(_.status == 0)
      .mapPartitions(iter => {
        val reader = new WKTReader(new GeometryFactory(new PrecisionModel(1000000.0)))
        iter.map(line => {
          val geom = reader.read(line.wkt)
          val coord = geom.getCoordinate
          TrackTarget(line.voyageID, Target(line.baseDateTime.getTime, coord.x, coord.y))
        })
      })
      .sort("track", "target")
      .groupBy("track")
      .agg(collect_list("target") as "targets")
      .as[TrackTargets]
      .flatMap(_.split(time, dist))
      .map(_.toTrackCells(origX, origY, cellSize))
      .repartition(conf.getInt("spark.app.output.repartition", 8))
      .write
      .mode(SaveMode.Overwrite)
      .parquet(conf.get("spark.app.output.path", "alluxio://localhost:19998/tracks"))

  } finally {
    spark.stop()
  }

}
