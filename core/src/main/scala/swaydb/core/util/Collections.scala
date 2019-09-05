/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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

package swaydb.core.util

import scala.reflect.ClassTag

object Collections {

  val emptyStringSeq = Seq.empty[String]

  implicit class IterableImplicit[T: ClassTag](items: Iterable[T]) {

    def foreachBreak(f: T => Boolean): Unit = {
      val iterator = items.iterator
      var break: Boolean = false
      while (iterator.hasNext && !break)
        break = f(iterator.next())
    }

    /**
     * Used for cases when multiple iterators over a list eg: collect & then fold is costly.
     */
    def foldLeftWhile[B](initial: B, condition: T => Boolean)(op: (B, T) => B): B = {
      var result = initial
      items foreachBreak {
        item =>
          val continue = condition(item)
          if (continue) result = op(result, item)
          !continue
      }
      result
    }
  }

  /**
   * Groups items ensuring if the input groupSize is > 1 then the output groups
   * should not contain a single item. Single items value merged into their previous gorup.
   */
  def groupedMergeSingles[T](groupSize: Int,
                             items: List[T]): List[List[T]] =
    if (groupSize <= 0) {
      List(items)
    } else {
      val grouped: List[List[T]] = items.grouped(groupSize).toList
      if (groupSize > 1 && grouped.size >= 2 && grouped.last.size == 1)
        grouped.dropRight(2) :+ (grouped(grouped.size - 2) ++ grouped.last)
      else
        grouped
    }

  /**
   * Groups items ensuring if the input groupSize is > 1 then the output groups
   * should not contain a single item. Single items value merged into their previous gorup.
   */
  def groupedMergeSingles[T](groupSize: Int,
                             items: List[T],
                             splitAt: Int): List[List[T]] = {
    val (itemsToGroupHead, itemsToGroupLast) = items.splitAt(splitAt)

    groupedMergeSingles(groupSize, itemsToGroupHead) ++
      groupedMergeSingles(groupSize, itemsToGroupLast)
  }
}
