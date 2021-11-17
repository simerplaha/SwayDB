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

package swaydb.java

import swaydb.slice.Slice
import swaydb.slice.order.KeyOrder
import swaydb.utils.Java.JavaFunction

import java.util.Comparator

object KeyComparator {
  final val lexicographic: KeyComparator[Slice[java.lang.Byte]] =
    new KeyComparator[Slice[java.lang.Byte]] {
      override def compare(o1: Slice[java.lang.Byte], o2: Slice[java.lang.Byte]): Int =
        KeyOrder.lexicographicJava.compare(o1, o2)
    }

  /**
   * Create a [[KeyComparator]] on type A when comparator for B is known and how to
   * convert A => B is known.
   */
  def by[A, B](comparator: Comparator[B], mapFunction: JavaFunction[A, B]): KeyComparator[A] =
    new KeyComparator[A] {
      val ordering = Ordering.by(mapFunction.apply)(Ordering.comparatorToOrdering(comparator))

      override def compare(o1: A, o2: A): Int =
        ordering.compare(o1, o2)
    }
}

trait KeyComparator[K] extends Comparator[K]
