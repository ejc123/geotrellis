/*
 * Copyright (c) 2014 Azavea.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geotrellis.feature.op.geometry

import geotrellis._
import geotrellis.feature._
import geotrellis.raster._

import spire.syntax.cfor._

/**
 * IDW Interpolation
 */
case class IDWInterpolate(points:Op[Seq[PointFeature[Int]]],re:Op[RasterExtent],radius:Op[Option[Int]]=None)
    extends Op3(points,re,radius)({
  (points,re,radius) =>
    val cols = re.cols
    val rows = re.rows
    val data = RasterData.emptyByType(TypeInt, cols, rows)
    if(points.isEmpty) {
      Result(Raster(data,re))
    } else {
      val r = radius match {
        case Some(r: Int) =>
          val rr = r*r
          val index: SpatialIndex[PointFeature[Int]] = SpatialIndex(points)(p => (p.geom.x,p.geom.y))

          cfor(0)(_ < cols, _ + 1) { col =>
            cfor(0)(_ < rows, _ + 1) { row =>
              val destX = re.gridColToMap(col)
              val destY = re.gridRowToMap(row)
              val pts = index.pointsInExtent(Extent(destX - r, destY - r, destX + r, destY + r))
              println(pts.size)
              if (pts.isEmpty) {
                data.set(col, row, NODATA)
              } else {
                var s = 0.0
                var c = 0
                var ws = 0.0
                val length = pts.length

                cfor(0)(_ < length, _ + 1) { i =>
                  val point = pts(i)
                  val dX = (destX - point.geom.x)
                  val dY = (destY - point.geom.y)
                  val d = dX * dX + dY * dY
                  if (d < rr) {
                    val w = 1 / d
                    s += point.data * w
                    ws += w
                    c += 1
                  }
                }

                if (c == 0) {
                  data.set(col, row, NODATA)
                } else {
                  val mean = s / ws
                  data.set(col, row, mean.toInt)
                }
              }
            }
          }
        case None =>
          val length = points.length
          cfor(0)(_ < cols, _ + 1) { col =>
            cfor(0)(_ < rows, _ + 1) { row =>
              val destX = re.gridColToMap(col)
              val destY = re.gridRowToMap(row)
              var s = 0.0
              var c = 0
              var ws = 0.0

              cfor(0)(_ < length, _ + 1) { i =>
                val point = points(i)
                val dX = (destX - point.geom.x)
                val dY = (destY - point.geom.y)
                val d = dX * dX + dY * dY
                val w = 1 / d
                s += point.data * w
                ws += w
                c += 1
              }

              if (c == 0) {
                data.set(col, row, NODATA)
              } else {
                val mean = s / ws
                data.set(col, row, mean.toInt)
              }
            }
          }
      }
      Result(Raster(data,re))
    }
})
