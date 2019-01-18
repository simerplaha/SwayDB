package swaydb.data.order

import swaydb.data.slice.Slice

object TimeOrder {
  val long = new TimeOrder[Slice[Byte]] {
    override def compare(left: Slice[Byte], right: Slice[Byte]): Int =
      if (left.isEmpty || right.isEmpty)
        1 //if either of them are empty then favour left to be the largest.
      else
        left.readLong() compare right.readLong()
  }
}

trait TimeOrder[T] extends Ordering[T]
