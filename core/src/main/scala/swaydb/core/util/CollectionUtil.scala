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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.util

import scala.reflect.ClassTag

object CollectionUtil {

  val emptyStringSeq = Seq.empty[String]

  implicit class IterableImplicit[T: ClassTag](it: Iterator[T]) {

    def foreachBreak(f: T => Boolean): Unit = {
      var break: Boolean = false
      while (it.hasNext && !break)
        break = f(it.next())
    }

    /**
      * Used for cases when multiple iterators over a list eg: collect & then fold is costly.
      */
    def foldLeftWhile[B](initial: B, condition: T => Boolean)(op: (B, T) => B): B = {
      var result = initial
      it foreachBreak {
        item =>
          val continue = condition(item)
          if (continue) result = op(result, item)
          !continue
      }
      result
    }
  }

  def groupedNoSingles[T](concurrentCompactions: Int,
                          items: List[T],
                          splitAt: Int): List[List[T]] = {
    def doSplit(items: List[T]): List[List[T]] = {
      val splitJobs: List[List[T]] = items.grouped(concurrentCompactions).toList
      if (splitJobs.size >= 2 && splitJobs.last.size == 1)
        splitJobs.dropRight(2) :+ (splitJobs(splitJobs.size - 2) ++ splitJobs.last)
      else
        splitJobs
    }

    val (itemsToGroupHead, itemsToGroupLast) = items.splitAt(splitAt)

    doSplit(itemsToGroupHead) ++ doSplit(itemsToGroupLast)
  }
}
