package geotrellis.spark.io.hadoop.formats

import org.apache.hadoop.io.{Writable, WritableComparable}

import scala.reflect.ClassTag

/** Trait that serves as implicit evidence that type K can be used as
  * a key when writing Hadoop MapFiles.
  */
abstract class HadoopWritable[K: Ordering] extends Serializable {
  type Writable <: org.apache.hadoop.io.Writable with WritableComparable[Writable]

  val writableClassTag: ClassTag[Writable]

  def toWritable(key: K): Writable
  def toValue(writable: Writable): K
  
  def newWritable(): Writable

  object implicits extends Serializable {
    implicit val writableClassTag: ClassTag[Writable] = HadoopWritable.this.writableClassTag

    implicit class ToWritableWrapper(val key: K) {
      def toWritable(): Writable = HadoopWritable.this.toWritable(key)
    }

    implicit class ToKeyWrapper(val writable: Writable) {
      def toValue(): K = HadoopWritable.this.toValue(writable)
    }

    implicit def keyOrdering = implicitly[Ordering[K]]

    implicit def writableOrdering[T <: Writable] = 
      new Ordering[Writable] {
        def compare(a: Writable, b: Writable) = a.compareTo(b)
      }
  }
}
