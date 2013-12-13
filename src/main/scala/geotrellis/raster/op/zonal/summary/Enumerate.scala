package geotrellis.raster.op.zonal.summary

import geotrellis._
import geotrellis.source._
import geotrellis.feature._
import geotrellis.feature.rasterize._
import geotrellis.statistics._

object Enumerate extends TileSummary[Vector[Long],List[Long],ValueSource[List[Long]]] {
  def handlePartialTile[D](pt:PartialTileIntersection[D]):Vector[Long] = {
    val PartialTileIntersection(r,polygons) = pt
    val holder = Vector[Long]()
    for(p <- polygons.asInstanceOf[List[Polygon[D]]]) {
      Rasterizer.foreachCellByFeature(p, r.rasterExtent)(
        new Callback[Geometry,D] {
          def apply (col:Int, row:Int, g:Geometry[D]) {
            holder :+ r.get(col,row)
          }
        }
      )
    }

    holder
  }

  def handleFullTile(ft:FullTileIntersection):Vector[Long] = {
    val holder = Vector[Long]()
    ft.tile.foreach(holder :+ _)
    holder
  }

  def converge(ds:DataSource[Vector[Long],_]) =
    ds.reduce(_++_).map(_.toList)
}

object EnumerateDouble extends TileSummary[Vector[Double],List[Double],ValueSource[List[Double]]] {
  def handlePartialTile[D](pt:PartialTileIntersection[D]):Vector[Double] = {
    val PartialTileIntersection(r,polygons) = pt
    val holder = Vector[Double]()
    for(p <- polygons.asInstanceOf[List[Polygon[D]]]) {
      Rasterizer.foreachCellByFeature(p, r.rasterExtent)(
        new Callback[Geometry,D] {
          def apply (col:Int, row:Int, g:Geometry[D]) {
            holder :+ r.get(col,row)
          }
        }
      )
    }

    holder
  }

  def handleFullTile(ft:FullTileIntersection):Vector[Double] = {
    val holder = Vector[Double]()
    ft.tile.foreachDouble(holder :+ _)
      holder
  }

  def converge(ds:DataSource[Vector[Double],_]) =
    ds.reduce(_++_).map(_.toList)
}
