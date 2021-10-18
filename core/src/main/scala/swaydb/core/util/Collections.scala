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

package swaydb.core.util

import swaydb.data.slice.Slice

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

private[swaydb] object Collections {

  val emptyStringSeq = Seq.empty[String]

  def hasOnlyOne[A](segments: Iterable[A]): Boolean = {
    val iterator = segments.iterator
    iterator.hasNext && {
      iterator.next()
      !iterator.hasNext //no next.
    }
  }

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
