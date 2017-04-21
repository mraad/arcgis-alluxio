package com.esri

import com.esri.hex.HexGrid
import com.esri.webmercator._
import com.vividsolutions.jts.geom.{GeometryFactory, PrecisionModel}
import com.vividsolutions.jts.io.WKTReader
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.util.ParseModes
import org.apache.spark.sql.types.StructType

object HexApp extends App {
  val spark = SparkSession
    .builder()
    .appName("Hex App")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  import spark.implicits._

  try {
    val conf = spark.sparkContext.getConf

    val schema = ScalaReflection.schemaFor[Broadcast].dataType.asInstanceOf[StructType]

    val size = conf.getInt("spark.app.size.meters", 200)

    spark
      .read
      .format("csv")
      .option("delimiter", conf.get("spark.app.input.delimiter", "\t"))
      .option("header", conf.getBoolean("spark.app.input.header", true))
      .option("mode", ParseModes.DROP_MALFORMED_MODE)
      .option("timestampFormat", conf.get("spark.app.input.timestampFormat", "yyyy-MM-dd HH:mm:ss"))
      .schema(schema)
      .load(conf.get("spark.app.input.path", "alluxio://localhost:19998/Broadcast.csv"))
      .as[Broadcast]
      .mapPartitions(iter => {
        val reader = new WKTReader(new GeometryFactory(new PrecisionModel(1000000.0)))
        iter.map(line => {
          val geom = reader.read(line.wkt)
          val coord = geom.getCoordinate
          BroadcastRow(line, coord.x, coord.y)
        })
      })
      .createTempView("BROADCASTS")

    spark.sqlContext.udf.register("toX", (d: Double) => {
      d toMercatorX
    })

    spark.sqlContext.udf.register("toY", (d: Double) => {
      d toMercatorY
    })

    val hexGrid = HexGrid(size, -20000000.0, -20000000.0)
    spark.sqlContext.udf.register("hex", (x: Double, y: Double) => {
      hexGrid.convertXYToRowCol(x, y).toLong
    })

    spark.sql("select hex,count(hex) as pop from" +
      "(select hex(toX(lon),toY(lat)) as hex from BROADCASTS where status=0)" +
      "group by hex")
      .repartition(conf.getInt("spark.app.output.repartition", 8))
      .write
      .format("csv")
      .save(conf.get("com.esri.output.path", s"alluxio://localhost:19998/hex$size"))

  } finally {
    spark.stop()
  }

}
