package geotrellis.raster.op.zonal.summary

import geotrellis._
import geotrellis.source._
import geotrellis.feature._
import geotrellis.feature.rasterize._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object Enumerate extends TileSummary[mutable.ArrayBuffer[((Int,Int),Int)],Array[Int],ValueSource[Array[Int]]] {
  def handlePartialTile[D](pt: PartialTileIntersection[D]): mutable.ArrayBuffer[((Int,Int),Int)] = {
    val PartialTileIntersection(r, polygons) = pt
    val holder = mutable.ArrayBuffer.empty[((Int,Int),Int)]
    for (p <- polygons.asInstanceOf[List[Polygon[D]]]) {
      Rasterizer.foreachCellByFeature(p, r.rasterExtent)(
        new Callback[Geometry, D] {
          def apply(col: Int, row: Int, g: Geometry[D]) {
            holder += Tuple2((col,row),r.get(col, row))
          }
        }
      )
    }
    holder
  }

  def handleFullTile(ft: FullTileIntersection): mutable.ArrayBuffer[((Int,Int),Int)] = {
    import scalaxy.loops._
    val holder = mutable.ArrayBuffer.empty[((Int,Int),Int)]
    val cols = ft.tile.cols
    val rows = ft.tile.rows
    for(col <- 0 until cols optimized) {
      for(row <- 0 until rows optimized) {
        holder += Tuple2((col,row),ft.tile.get(col,row))
      }
    }
    holder
  }

  def converge(ds: DataSource[mutable.ArrayBuffer[((Int,Int),Int)], _]):ValueSource[Array[Int]] =
    ds.foldLeft(mutable.ArrayBuffer.empty[((Int,Int),Int)])(_ ++ _).map(a => a.sortWith(compare).foldLeft(mutable.ArrayBuffer.empty[Int])((a,b) => (a += b._2)).toArray)

  def compare(a:((Int,Int),Int), b:((Int,Int),Int)) = (a._1,b._1) match {
    case (a,b) if (a._1 < b._1 ||(a._1 == b._1 && a._2 < b._2)) => true
    case (a,b) if (a._1 > b._1 ||(a._1 == b._1 && a._2 > b._2)) => false
    case (a,b) if (a._1==b._1 && a._2 ==b._2) => false
  }
}



object EnumerateDouble extends TileSummary[mutable.ArrayBuffer[(Int,Int,Double)],Array[(Int,Int,Double)],ValueSource[Array[(Int,Int,Double)]]] {
  def handlePartialTile[D](pt: PartialTileIntersection[D]): mutable.ArrayBuffer[(Int,Int,Double)] = {
    val PartialTileIntersection(r, polygons) = pt
    var holder = mutable.ArrayBuffer.empty[(Int,Int,Double)]
    for (p <- polygons.asInstanceOf[List[Polygon[D]]]) {
      Rasterizer.foreachCellByFeature(p, r.rasterExtent)(
        new Callback[Geometry, D] {
          def apply(col: Int, row: Int, g: Geometry[D]) {
            holder += Tuple3(col,row,r.getDouble(col, row))
          }
        }
      )
    }
    holder
  }

  def handleFullTile(ft: FullTileIntersection): mutable.ArrayBuffer[(Int,Int,Double)] = {
    import scalaxy.loops._
    val holder = mutable.ArrayBuffer.empty[(Int,Int,Double)]
    val cols = ft.tile.cols
    val rows = ft.tile.rows
    for(col <- 0 until cols optimized) {
      for(row <- 0 until rows optimized) {
        holder += Tuple3(col,row,ft.tile.getDouble(col,row))
      }
    }
    holder
  }

  def converge(ds: DataSource[mutable.ArrayBuffer[(Int,Int,Double)], _]) =
    ds.foldLeft(mutable.ArrayBuffer.empty[(Int,Int,Double)])(_ ++ _).map(_.toArray)
}
