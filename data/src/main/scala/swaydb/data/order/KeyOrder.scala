package swaydb.data.order

import swaydb.data.slice.Slice

object KeyOrder {

  /**
    * Default ordering.
    *
    * Custom key-ordering can be implemented.
    * Documentation: http://www.swaydb.io/custom-key-ordering
    *
    */
  val default = new KeyOrder[Slice[Byte]] {
    def compare(a: Slice[Byte], b: Slice[Byte]): Int = {
      val minimum = java.lang.Math.min(a.written, b.written)
      var i = 0
      while (i < minimum) {
        val aB = a(i) & 0xFF
        val bB = b(i) & 0xFF
        if (aB != bB) return aB - bB
        i += 1
      }
      a.written - b.written
    }
  }

  /**
    * Provides the default reverse ordering.
    */
  val reverse = new KeyOrder[Slice[Byte]] {
    def compare(a: Slice[Byte], b: Slice[Byte]): Int =
      default.compare(a, b) * -1
  }

  def apply[K](ordering: Ordering[K]): KeyOrder[K] = new KeyOrder[K]() {
    override def compare(x: K, y: K): Int =
      ordering.compare(x, y)
  }

}

trait KeyOrder[K] extends Ordering[K]
