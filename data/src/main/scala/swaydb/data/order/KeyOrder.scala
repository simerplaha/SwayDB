/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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

package swaydb.data.order

import swaydb.slice.Slice

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
      def compare(a: Slice[Byte], b: Slice[Byte]): Int =
        KeyOrder.defaultCompare(
          a = a,
          b = b,
          maxBytes = Math.min(a.size, b.size)
        )
    }

  val lexicographicJava: KeyOrder[Slice[java.lang.Byte]] =
    new KeyOrder[Slice[java.lang.Byte]] {
      def compare(a: Slice[java.lang.Byte], b: Slice[java.lang.Byte]): Int =
        KeyOrder.defaultCompareJava(
          a = a,
          b = b,
          maxBytes = Math.min(a.size, b.size)
        )
    }

  @inline def defaultCompare(a: Slice[Byte], b: Slice[Byte], maxBytes: Int): Int = {
    var i = 0
    while (i < maxBytes) {
      val aB = a(i) & 0xFF
      val bB = b(i) & 0xFF
      if (aB != bB) return aB - bB
      i += 1
    }
    a.size - b.size
  }

  @inline def defaultCompareJava(a: Slice[java.lang.Byte], b: Slice[java.lang.Byte], maxBytes: Int): Int = {
    var i = 0
    while (i < maxBytes) {
      val aB = a(i) & 0xFF
      val bB = b(i) & 0xFF
      if (aB != bB) return aB - bB
      i += 1
    }
    a.size - b.size
  }

  /**
   * Provides the default reverse ordering.
   */
  val reverseLexicographic: KeyOrder[Slice[Byte]] =
    new KeyOrder[Slice[Byte]] {
      def compare(a: Slice[Byte], b: Slice[Byte]): Int =
        default.compare(a, b) * -1
    }

  /**
   * Provides the default reverse ordering.
   */

  val integer: KeyOrder[Slice[Byte]] =
    new KeyOrder[Slice[Byte]] {
      override def compare(x: Slice[Byte], y: Slice[Byte]): Int =
        x.readInt() compare y.readInt()
    }

  val long: KeyOrder[Slice[Byte]] =
    new KeyOrder[Slice[Byte]] {
      override def compare(x: Slice[Byte], y: Slice[Byte]): Int =
        x.readLong() compare y.readLong()
    }

  def by[T, S](f: T => S)(implicit ordering: Ordering[S]): KeyOrder[T] =
    KeyOrder(Ordering.by(f)(ordering))

  def apply[K](ordering: Ordering[K]): KeyOrder[K] =
    new KeyOrder[K]() {
      override def compare(x: K, y: K): Int =
        ordering.compare(x, y)
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
