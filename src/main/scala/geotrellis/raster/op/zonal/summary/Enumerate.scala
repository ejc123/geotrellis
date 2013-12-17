package geotrellis.raster.op.zonal.summary

import geotrellis._
import geotrellis.source._
import geotrellis.feature._
import geotrellis.feature.rasterize._
import collection.mutable.ListBuffer

object Enumerate extends TileSummary[ListBuffer[Int],List[Int],ValueSource[List[Int]]] {
  def handlePartialTile[D](pt:PartialTileIntersection[D]):ListBuffer[Int] = {
    val PartialTileIntersection(r,polygons) = pt
    val holder = ListBuffer.empty[Int]
    for(p <- polygons.asInstanceOf[List[Polygon[D]]]) {
      Rasterizer.foreachCellByFeature(p, r.rasterExtent)(
        new Callback[Geometry,D] {
          def apply (col:Int, row:Int, g:Geometry[D]) {
            holder += r.get(col,row)
          }
        }
      )
    }
    holder
  }

  def handleFullTile(ft:FullTileIntersection):ListBuffer[Int] = {
    val holder = ListBuffer.empty[Int]
    ft.tile.foreach(holder += _)
    holder
  }

  def converge(ds:DataSource[ListBuffer[Int],_]) =
    ds.reduce(_++_).map(_.toList)
}

object EnumerateDouble extends TileSummary[ListBuffer[Double],List[Double],ValueSource[List[Double]]] {
  def handlePartialTile[D](pt:PartialTileIntersection[D]):ListBuffer[Double] = {
    val PartialTileIntersection(r,polygons) = pt
    var holder = ListBuffer.empty[Double]
    for(p <- polygons.asInstanceOf[List[Polygon[D]]]) {
      Rasterizer.foreachCellByFeature(p, r.rasterExtent)(
        new Callback[Geometry,D] {
          def apply (col:Int, row:Int, g:Geometry[D]) {
            holder += r.get(col,row)
          }
        }
      )
    }
    holder
  }

  def handleFullTile(ft:FullTileIntersection):ListBuffer[Double] = {
    val holder = ListBuffer.empty[Double]
    ft.tile.foreachDouble(holder += _)
      holder
  }

  def converge(ds:DataSource[ListBuffer[Double],_]) =
    ds.reduce(_++_).map(_.toList)
}
