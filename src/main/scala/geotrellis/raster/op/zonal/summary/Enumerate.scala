package geotrellis.raster.op.zonal.summary

import geotrellis._
import geotrellis.source._
import geotrellis.feature._
import geotrellis.feature.rasterize._
import collection.mutable.ListBuffer

object Enumerate extends TileSummary[ListBuffer[Long],List[Long],ValueSource[List[Long]]] {
  def handlePartialTile[D](pt:PartialTileIntersection[D]):ListBuffer[Long] = {
    val PartialTileIntersection(r,polygons) = pt
    val holder = ListBuffer.empty[Long]
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

  def handleFullTile(ft:FullTileIntersection):ListBuffer[Long] = {
    val holder = ListBuffer.empty[Long]
    ft.tile.foreach(holder += _)
    holder
  }

  def converge(ds:DataSource[ListBuffer[Long],_]) =
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
