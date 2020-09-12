/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * This file is a part of SwayDB.
 *
 * SwayDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * SwayDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.data.order

import swaydb.data.slice.Slice._
object KeyOrder {

  /**
   * Default ordering.
   *
   * Custom key-ordering can be implemented.
   * Documentation: http://www.swaydb.io/custom-key-ordering
   *
   */
  val default, lexicographic: KeyOrder[Sliced[Byte]] =
    new KeyOrder[Sliced[Byte]] {
      def compare(a: Sliced[Byte], b: Sliced[Byte]): Int =
        KeyOrder.defaultCompare(
          a = a,
          b = b,
          maxBytes = Math.min(a.size, b.size)
        )
    }

  @inline def defaultCompare(a: Sliced[Byte], b: Sliced[Byte], maxBytes: Int): Int = {
    var i = 0
    while (i < maxBytes) {
      val aB = a.getC(i) & 0xFF
      val bB = b.getC(i) & 0xFF
      if (aB != bB) return aB - bB
      i += 1
    }
    a.size - b.size
  }

  /**
   * Provides the default reverse ordering.
   */
  val reverse: KeyOrder[Sliced[Byte]] =
    new KeyOrder[Sliced[Byte]] {
      def compare(a: Sliced[Byte], b: Sliced[Byte]): Int =
        default.compare(a, b) * -1
    }

  def apply[K](ordering: Ordering[K]): KeyOrder[K] =
    new KeyOrder[K]() {
      override def compare(x: K, y: K): Int =
        ordering.compare(x, y)
    }

  val integer: KeyOrder[Sliced[Byte]] =
    new KeyOrder[Sliced[Byte]] {
      override def compare(x: Sliced[Byte], y: Sliced[Byte]): Int =
        x.readInt() compare y.readInt()
    }

  val long: KeyOrder[Sliced[Byte]] =
    new KeyOrder[Sliced[Byte]] {
      override def compare(x: Sliced[Byte], y: Sliced[Byte]): Int =
        x.readLong() compare y.readLong()
    }
}

trait KeyOrder[K] extends Ordering[K] {
  /**
   * For internal use only.
   *
   * The key is used to create secondary indexes like HashIndex and BloomFilters.
   *
   * Useful for partially ordered keys.
   *
   * For example if your key is
   * {{{
   *   case class MyData(id: Int, name: Option[String]))
   * }}}
   *
   * Suppose your key is MyData(1, "John") and your [[KeyOrder]] if on MyData.id
   * then this [[comparableKey]] should return MyData(1, None) so you can search for
   * keys like MyData(1, None) and get result MyData(1, "John").
   *
   * This is called partial ordering and full reads on partial keys so you
   * can store extra data with key without having to read the value.
   */
  private[swaydb] def comparableKey(key: K): K = key
}
