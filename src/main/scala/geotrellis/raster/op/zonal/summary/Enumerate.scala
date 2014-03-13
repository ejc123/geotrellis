package geotrellis.raster.op.zonal.summary

import geotrellis._
import geotrellis.source._
import geotrellis.feature._
import geotrellis.feature.rasterize._
import scala.collection.mutable

object Enumerate extends TileSummary[mutable.ArrayBuffer[Int],Array[Int],ValueSource[Array[Int]]] {
  def handlePartialTile[D](pt: PartialTileIntersection[D]): mutable.ArrayBuffer[Int] = {
    val PartialTileIntersection(r, polygons) = pt
    val holder = mutable.ArrayBuffer.empty[Int]
    for (p <- polygons.asInstanceOf[List[Polygon[D]]]) {
      Rasterizer.foreachCellByFeature(p, r.rasterExtent)(
        new Callback[Geometry, D] {
          def apply(col: Int, row: Int, g: Geometry[D]) {
            holder += r.get(col, row)
          }
        }
      )
    }
    holder
  }

  def handleFullTile(ft: FullTileIntersection): mutable.ArrayBuffer[Int] = {
    val holder = mutable.ArrayBuffer.empty[Int]
    ft.tile.foreach(holder += _)
    holder
  }

  def converge(ds: DataSource[mutable.ArrayBuffer[Int], _]) =
    ds.foldLeft(mutable.ArrayBuffer.empty[Int])(_ ++ _).map(_.toArray)
}

object EnumerateDouble extends TileSummary[mutable.ArrayBuffer[Double],Array[Double],ValueSource[Array[Double]]] {
  def handlePartialTile[D](pt: PartialTileIntersection[D]): mutable.ArrayBuffer[Double] = {
    val PartialTileIntersection(r, polygons) = pt
    var holder = mutable.ArrayBuffer.empty[Double]
    for (p <- polygons.asInstanceOf[List[Polygon[D]]]) {
      Rasterizer.foreachCellByFeature(p, r.rasterExtent)(
        new Callback[Geometry, D] {
          def apply(col: Int, row: Int, g: Geometry[D]) {
            holder += r.get(col, row)
          }
        }
      )
    }
    holder
  }

  def handleFullTile(ft: FullTileIntersection): mutable.ArrayBuffer[Double] = {
    val holder = mutable.ArrayBuffer.empty[Double]
    ft.tile.foreachDouble(holder += _)
    holder
  }

  def converge(ds: DataSource[mutable.ArrayBuffer[Double], _]) =
    ds.foldLeft(mutable.ArrayBuffer.empty[Double])(_ ++ _).map(_.toArray)
}
