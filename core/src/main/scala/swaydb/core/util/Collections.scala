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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.core.util

import swaydb.data.slice.Slice

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

private[swaydb] object Collections {

  val emptyStringSeq = Seq.empty[String]

  def ensureNotNull[T](buffer: ListBuffer[T]): ListBuffer[T] =
    if (buffer == null)
      ListBuffer.empty[T]
    else
      buffer

  implicit class IterableImplicit[T](items: Iterable[T]) {

    @inline final def foreachBreak(f: T => Boolean): Unit = {
      val iterator = items.iterator
      var break: Boolean = false
      while (iterator.hasNext && !break)
        break = f(iterator.next())
    }

    /**
     * Used for cases when multiple iterators over a list eg: collect & then fold is costly.
     */
    @inline final def foldLeftWhile[B](initial: B, condition: T => Boolean)(op: (B, T) => B): B = {
      var result = initial
      items foreachBreak {
        item =>
          val continue = condition(item)
          if (continue) result = op(result, item)
          !continue
      }
      result
    }

    @inline final def untilSome[R](f: T => Option[R]): Option[(R, T)] = {
      items foreach {
        item =>
          f(item) match {
            case Some(value) =>
              //Not a good idea to break out with return. Needs improvement.
              return Some((value, item))

            case None =>
            //continue reading
          }
      }
      None
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
   * should not contain a single item. Single items value merged into their previous group.
   */
  def groupedMergeSingles[T](groupSize: Int,
                             items: List[T],
                             splitAt: Int): List[List[T]] = {
    val (itemsToGroupHead, itemsToGroupLast) = items.splitAt(splitAt)

    groupedMergeSingles(groupSize, itemsToGroupHead) ++
      groupedMergeSingles(groupSize, itemsToGroupLast)
  }

  def groupedBySize[T: ClassTag](minGroupSize: Int,
                                 itemSize: T => Int,
                                 items: Slice[T]): Slice[Slice[T]] =
    if (minGroupSize <= 0) {
      Slice(items)
    } else {
      val allGroups =
        Slice
          .of[Slice[T]](items.size)
          .add(Slice.of[T](items.size))

      var currentGroupSize: Int = 0

      var i = 0
      while (i < items.size) {
        if (currentGroupSize < minGroupSize) {
          val currentItem = items(i)
          allGroups.last add currentItem
          currentGroupSize += itemSize(currentItem)
          i += 1
        } else {
          val tailItemsSize = items.drop(i).foldLeft(0)(_ + itemSize(_))
          if (tailItemsSize >= minGroupSize) {
            val newGroup = Slice.of[T](items.size - i + 1)
            allGroups add newGroup
            currentGroupSize = 0
          } else {
            currentGroupSize = minGroupSize - 1
          }
        }
      }

      allGroups
    }
}
