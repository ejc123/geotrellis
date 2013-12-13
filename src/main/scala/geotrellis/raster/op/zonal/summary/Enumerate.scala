package geotrellis.raster.op.zonal.summary

import geotrellis._
import geotrellis.source._
import geotrellis.feature._
import geotrellis.feature.rasterize._
import geotrellis.statistics._

object Enumerate extends TileSummary[Array[Long],List[Long],ValueSource[List[Long]]] {
  def handlePartialTile[D](pt:PartialTileIntersection[D]):Array[Long] = {
    val PartialTileIntersection(r,polygons) = pt
    val holder = Array[Long]()
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

  def handleFullTile(ft:FullTileIntersection):Array[Long] = {
    val holder = Array[Long]()
    ft.tile.foreach(holder :+ _)
    holder
  }

  def converge(ds:DataSource[Array[Long],_]) =
    ds.reduce(_.toList ++ _.toList)
}

object EnumerateDouble extends TileSummary[Array[Double],List[Double],ValueSource[List[Double]]] {
  def handlePartialTile[D](pt:PartialTileIntersection[D]):Array[Double] = {
    val PartialTileIntersection(r,polygons) = pt
    val holder = Array[Double]()
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

  def handleFullTile(ft:FullTileIntersection):Array[Double] = {
    val holder = Array[Double]()
    ft.tile.foreachDouble(holder :+ _)
      holder
  }

  def converge(ds:DataSource[Array[Double],_]) =
    ds.reduce(_.toList ++ _.toList)
}
