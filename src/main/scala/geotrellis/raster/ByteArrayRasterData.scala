package geotrellis.raster

import geotrellis._

import java.nio.ByteBuffer

/**
 * RasterData based on Array[Byte] (each cell as a Byte).
 */
final case class ByteArrayRasterData(array: Array[Byte], cols: Int, rows: Int)
  extends MutableRasterData with IntBasedArray {
  def getType = TypeByte
  def alloc(cols: Int, rows: Int) = ByteArrayRasterData.ofDim(cols, rows)
  val length = array.length
  def apply(i: Int) = b2i(array(i))
  def update(i: Int, z: Int) { array(i) = i2b(z) }
  def copy = ByteArrayRasterData(array.clone, cols, rows)

  def toArrayByte: Array[Byte] = array.clone

  def warp(current:RasterExtent,target:RasterExtent):RasterData = {
    val warped = Array.ofDim[Byte](target.cols*target.rows).fill(byteNODATA)
    Warp(current,target,new ByteBufferWarpAssign(ByteBuffer.wrap(array),warped))
    ByteArrayRasterData(warped, target.cols, target.rows)
  }
}

object ByteArrayRasterData {
  def ofDim(cols: Int, rows: Int) = 
    new ByteArrayRasterData(Array.ofDim[Byte](cols * rows), cols, rows)
  def empty(cols: Int, rows: Int) = 
    new ByteArrayRasterData(Array.ofDim[Byte](cols * rows).fill(byteNODATA), cols, rows)

  def fromArrayByte(bytes: Array[Byte], cols: Int, rows: Int) = ByteArrayRasterData(bytes, cols, rows)
}

