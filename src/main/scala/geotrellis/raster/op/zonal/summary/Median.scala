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
  def median = {
    Sorting.quickSort(values)
    if (values.length == 0) {
      geotrellis.NODATA
    } else {
      if (values.length % 2 == 0) {
        (values(values.length / 2) + values(values.length / 2 - 1)) / 2
      } else {
        values(values.length / 2)
      }
    }
  }

  def +(b: IntMedian) = IntMedian((values ++ b.values))
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

  def medianTiledRaster(r: Raster): IntMedian = IntMedian(r.toArray)
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
      var values: Array[Int] = Array[Int]()
      val f = new Callback[Geometry, D] {
        def apply(col: Int, row: Int, g: Geometry[D]) {
          val z = r.get(col, row)
            values = values :+ z
        }
      }

      geotrellis.feature.rasterize.Rasterizer.foreachCellByFeature(
        g,
        r.rasterExtent)(f)
      IntMedian(values)
    }))

  def handleFullTile(rOp: Op[Raster]) = rOp.map(r =>
    tileResults.get(r.rasterExtent).getOrElse( IntMedian(r.toArray) ))


  def handleNoDataTile = IntMedian(Array[Int](geotrellis.NODATA))

  def reducer(mapResults: List[IntMedian]): Int = mapResults.foldLeft(IntMedian(Array[Int]()))(_ + _).median
}

case class DoubleMedian(values: Array[Double]) {
  def median = {
    Sorting.quickSort(values)
    if (values.length == 0) {
      geotrellis.NODATA
    } else {
      if (values.length % 2 == 0) {
        (values(values.length / 2) + values(values.length / 2 - 1)) / 2
      } else {
        values(values.length / 2)
      }
    }
  }

  def +(b: DoubleMedian) = DoubleMedian((values ++ b.values))
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

  def medianTiledRaster(r: Raster): DoubleMedian = DoubleMedian(r.toArrayDouble)
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
      var values = Array[Double]()
      val f = new Callback[Geometry, D] {
        def apply(col: Int, row: Int, g: Geometry[D]) {
          val z = r.getDouble(col, row)
          // if (!java.lang.Double.isNaN(z)) {
            values = values :+ z
          // }
        }
      }
      geotrellis.feature.rasterize.Rasterizer.foreachCellByFeature(
        g,
        r.rasterExtent)(f)
      DoubleMedian(values)
    }))
  }

  def handleFullTile(rOp: Op[Raster]) = rOp.map(r =>
    tileResults.get(r.rasterExtent).getOrElse( DoubleMedian(r.toArrayDouble) ))

  def handleNoDataTile = DoubleMedian(Array[Double](Double.NaN))

  def reducer(mapResults: List[DoubleMedian]): Double = mapResults.foldLeft(DoubleMedian(Array[Double]()))(_ + _).median
}
