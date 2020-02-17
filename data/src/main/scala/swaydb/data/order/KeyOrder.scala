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
 */

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
  val default, lexicographic: KeyOrder[Slice[Byte]] =
    new KeyOrder[Slice[Byte]] {
      def compare(a: Slice[Byte], b: Slice[Byte]): Int = {
        val minimum = Math.min(a.size, b.size)
        var i = 0
        while (i < minimum) {
          val aB = a.getC(i) & 0xFF
          val bB = b.getC(i) & 0xFF
          if (aB != bB) return aB - bB
          i += 1
        }
        a.size - b.size
      }

      private[swaydb] override def comparableKey(data: Slice[Byte]): Slice[Byte] =
        data
    }

  /**
   * Provides the default reverse ordering.
   */
  val reverse: KeyOrder[Slice[Byte]] =
    new KeyOrder[Slice[Byte]] {
      def compare(a: Slice[Byte], b: Slice[Byte]): Int =
        default.compare(a, b) * -1

      private[swaydb] override def comparableKey(data: Slice[Byte]): Slice[Byte] =
        data
    }

  def apply[K](ordering: Ordering[K]): KeyOrder[K] =
    new KeyOrder[K]() {
      override def compare(x: K, y: K): Int =
        ordering.compare(x, y)

      private[swaydb] override def comparableKey(data: K): K =
        data
    }

  val integer: KeyOrder[Slice[Byte]] =
    new KeyOrder[Slice[Byte]] {
      override def compare(x: Slice[Byte], y: Slice[Byte]): Int =
        x.readInt() compare y.readInt()

      private[swaydb] override def comparableKey(data: Slice[Byte]): Slice[Byte] =
        data
    }

  val long: KeyOrder[Slice[Byte]] =
    new KeyOrder[Slice[Byte]] {
      override def compare(x: Slice[Byte], y: Slice[Byte]): Int =
        x.readLong() compare y.readLong()

      private[swaydb] override def comparableKey(data: Slice[Byte]): Slice[Byte] =
        data
    }
}

trait KeyOrder[K] extends Ordering[K] {
  /**
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
  private[swaydb] def comparableKey(data: K): K = data
}
