package geotrellis.raster.op.zonal.summary

import geotrellis._
import geotrellis.feature._
import geotrellis.feature.rasterize._
import geotrellis.data._
import scala.math.{max, min}
import geotrellis.raster.TileArrayRasterData
import geotrellis.raster.TiledRasterData
import scala.util.Sorting

case class IntMedian(values: Array[Int]) {
  def median = if (values.length == 0) {
    geotrellis.NODATA
  } else {
    if (values.length % 2 == 0) {
      (values(values.length / 2) + values(values.length / 2 - 1)) / 2
    } else {
      values(values.length / 2)
    }
  }

  def +(b: IntMedian) = IntMedian((values ++ b.values).sorted)
}

object Median {

  def createTileResults(trd: TiledRasterData, re: RasterExtent) = {
    trd.getTiles(re)
      .map {
      r => (r.rasterExtent, medianTiledRaster(r))
    }
      .toMap
  }

  def medianRaster(r: Raster): Int = medianTiledRaster(r).median

  def medianTiledRaster(r: Raster): IntMedian = IntMedian(r.toArray.sorted)
}

/**
 * Perform a zonal summary that calculates the median of all raster cells within a geometry.
 * This operation is for integer typed Rasters. If you want the Median for double type rasters
 * (TypeFloat,TypeDouble) use [[MedianDouble]]
 *
 * @param   r             Raster to summarize
 * @param   zonePolygon   Polygon that defines the zone
 * @param   tileResults   Cached results of full tiles created by createTileResults
 */
case class Median[DD](r: Op[Raster], zonePolygon: Op[Polygon[DD]], tileResults: Map[RasterExtent, IntMedian])
                     (implicit val mB: Manifest[IntMedian], val mD: Manifest[DD]) extends TiledPolygonalZonalSummary[Int] {

  type B = IntMedian
  type D = DD

  def handlePartialTileIntersection(rOp: Op[Raster], gOp: Op[Geometry[D]]) =
    rOp.flatMap(r => gOp.flatMap(g => {
      var values: Array[Int] = new Array[Int](0)
      val f = new Callback[Geometry, D] {
        def apply(col: Int, row: Int, g: Geometry[D]) {
          val z = r.get(col, row)
          if (z != NODATA) {
            values = values :+ z
          }
        }
      }

      geotrellis.feature.rasterize.Rasterizer.foreachCellByFeature(
        g,
        r.rasterExtent)(f)
      IntMedian(values.sorted)
    }))

  def handleFullTile(rOp: Op[Raster]) = rOp.map(r =>
    tileResults.get(r.rasterExtent).getOrElse({
      var values = new Array[Int](0)
      r.force.foreach((x: Int) => if (x != NODATA) values = values :+ x)
      IntMedian(values.sorted)
    }))


  def handleNoDataTile = IntMedian(new Array[Int](0))

  def reducer(mapResults: List[IntMedian]): Int = mapResults.foldLeft(IntMedian(new Array[Int](0)))(_ + _).median
}

case class DoubleMedian(values: Array[Double]) {
  def median = if (values.length == 0) {
    geotrellis.NODATA
  } else {
    if (values.length % 2 == 0) {
      (values(values.length / 2) + values(values.length / 2 - 1)) / 2
    } else {
      values(values.length / 2)
    }
  }

  def +(b: DoubleMedian) = DoubleMedian((values ++ b.values).sorted)
}

object MedianDouble {
  def createTileResults(trd: TiledRasterData, re: RasterExtent) = {
    trd.getTiles(re)
      .map {
      r => (r.rasterExtent, medianTiledRaster(r))
    }
      .toMap
  }

  def medianRaster(r: Raster): Double = medianTiledRaster(r).median

  def medianTiledRaster(r: Raster): DoubleMedian = DoubleMedian(r.toArrayDouble.sorted)
}

/**
 * Perform a zonal summary that calculates the median of all raster cells within a geometry.
 *
 * @param   r             Raster to summarize
 * @param   zonePolygon   Polygon that defines the zone
 * @param   tileResults   Cached results of full tiles created by createTileResults
 */
case class MedianDouble[DD](r: Op[Raster], zonePolygon: Op[Polygon[DD]], tileResults: Map[RasterExtent, DoubleMedian])
                           (implicit val mB: Manifest[DoubleMedian], val mD: Manifest[DD]) extends TiledPolygonalZonalSummary[Double] {

  type B = DoubleMedian
  type D = DD

  def handlePartialTileIntersection(rOp: Op[Raster], gOp: Op[Geometry[D]]) = {
    rOp.flatMap(r => gOp.flatMap(g => {
      var values = new Array[Double](0)
      val f = new Callback[Geometry, D] {
        def apply(col: Int, row: Int, g: Geometry[D]) {
          val z = r.getDouble(col, row)
          if (!java.lang.Double.isNaN(z)) {
            values = values :+ z
          }
        }
      }
      geotrellis.feature.rasterize.Rasterizer.foreachCellByFeature(
        g,
        r.rasterExtent)(f)
      DoubleMedian(values.sorted)
    }))
  }

  def handleFullTile(rOp: Op[Raster]) = rOp.map(r =>
    tileResults.get(r.rasterExtent).getOrElse({
      var values = new Array[Double](0)
      r.force.foreachDouble((x: Double) => if (!java.lang.Double.isNaN(x))  values = values :+ x.toDouble)
      DoubleMedian(values.sorted)
    }))

  def handleNoDataTile = DoubleMedian(new Array[Double](0))

  def reducer(mapResults: List[DoubleMedian]): Double = mapResults.foldLeft(DoubleMedian(new Array[Double](0)))(_ + _).median
}
