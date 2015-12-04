/*
 * Copyright (c) 2014 DigitalGlobe.
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

package geotrellis.spark.op.local

import geotrellis.spark._
import geotrellis.raster.op.local.Subtract
import geotrellis.spark.rdd.RasterRDD

trait SubtractOpMethods[+Repr <: RasterRDD] { self: Repr =>
  /** Subtract a constant value from each cell.*/
  def localSubtract(i: Int) = 
    self.mapTiles { case Tile(t, r) => Tile(t, Subtract(r, i)) }
  /** Subtract a constant value from each cell.*/
  def -(i:Int) = localSubtract(i)
  /** Subtract each value of a cell from a constant value. */
  def localSubtractFrom(i: Int) = 
    self.mapTiles { case Tile(t, r) => Tile(t, Subtract(i, r)) }
  /** Subtract each value of a cell from a constant value. */
  def -:(i:Int) = localSubtractFrom(i)
  /** Subtract a double constant value from each cell.*/
  def localSubtract(d: Double) = 
    self.mapTiles { case Tile(t, r) => Tile(t, Subtract(r, d)) }
  /** Subtract a double constant value from each cell.*/
  def -(d:Double) = localSubtract(d)
  /** Subtract each value of a cell from a double constant value. */
  def localSubtractFrom(d: Double) = 
    self.mapTiles { case Tile(t, r) => Tile(t, Subtract(d, r)) }
  /** Subtract each value of a cell from a double constant value. */
  def -:(d:Double) = localSubtractFrom(d)
  /** Subtract the values of each cell in each raster. */
  def localSubtract(rdd: RasterRDD) = 
    self.combineTiles(rdd) { case (Tile(t1, r1), Tile(t2, r2)) => Tile(t1, Subtract(r1, r2)) }
  /** Subtract the values of each cell in each raster. */
  def -(rdd: RasterRDD) = localSubtract(rdd)
  /** Subtract the values of each cell in each raster. */
}
