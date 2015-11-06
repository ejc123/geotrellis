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

trait MinRasterSourceMethods extends RasterSourceMethods {
  /** Min a constant Int value to each cell. */
  def localMin(i: Int): RasterSource = rasterSource.mapTile(Min(_, i))
  /** Min a constant Double value to each cell. */
  def localMin(d: Double): RasterSource = rasterSource.mapTile(Min(_, d))
  /** Min the values of each cell in each raster.  */
  def localMin(rs: RasterSource): RasterSource = rasterSource.combineTile(rs)(Min(_, _))
  /** Min the values of each cell in each raster.  */
  def localMin(rss: Seq[RasterSource]): RasterSource = rasterSource.combineTile(rss)(Min(_))
}
