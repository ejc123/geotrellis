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

package geotrellis.engine.op.local

import geotrellis.engine._
import geotrellis.raster.op.local._

trait SubtractRasterSourceMethods extends RasterSourceMethods {
  /** Subtract a constant value from each cell.*/
  def localSubtract(i: Int): RasterSource = rasterSource.mapTile(Subtract(_, i))
  /** Subtract a constant value from each cell.*/
  def -(i: Int) = localSubtract(i)
  /** Subtract each value of a cell from a constant value. */
  def localSubtractFrom(i: Int): RasterSource = rasterSource.mapTile(Subtract(i, _))
  /** Subtract each value of a cell from a constant value. */
  def -:(i: Int) = localSubtract(i)
  /** Subtract a double constant value from each cell.*/
  def localSubtract(d: Double): RasterSource = rasterSource.mapTile(Subtract(_, d))
  /** Subtract a double constant value from each cell.*/
  def -(d: Double) = localSubtract(d)
  /** Subtract each value of a cell from a double constant value. */
  def localSubtractFrom(d: Double): RasterSource = rasterSource.mapTile(Subtract(d, _))
  /** Subtract each value of a cell from a double constant value. */
  def -:(d: Double) = localSubtractFrom(d)
  /** Subtract the values of each cell in each raster. */
  def localSubtract(rs: RasterSource): RasterSource = rasterSource.combineTile(rs)(Subtract(_, _))
  /** Subtract the values of each cell in each raster. */
  def -(rs: RasterSource): RasterSource = localSubtract(rs)
  /** Subtract the values of each cell in each raster. */
  def localSubtract(rss: Seq[RasterSource]): RasterSource = rasterSource.combineTile(rss)(Subtract(_))
  /** Subtract the values of each cell in each raster. */
  def -(rss:Seq[RasterSource]): RasterSource = localSubtract(rss)
}
