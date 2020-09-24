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
 * If you modify this Program, or any covered work, by linking or combining
 * it with other code, such other code is not for that reason alone subject
 * to any of the requirements of the GNU Affero GPL version 3.
 */

package swaydb.core.util.series.growable

import scala.reflect.ClassTag

object SeriesGrowable {

  @inline def volatile[T >: Null : ClassTag](lengthPerSlice: Int): SeriesGrowable[T] = {
    val sizePerSeries = lengthPerSlice max 1 //size cannot be 0

    val series = SeriesGrowVolatile[SeriesGrowVolatile[T]](2)
    val items = SeriesGrowVolatile[T](sizePerSeries)
    series.add(items)

    new SeriesGrowable(
      series = series,
      lengthPerSlice = sizePerSeries
    )
  }

  @inline private def extend[T >: Null : ClassTag](series: SeriesGrowVolatile[SeriesGrowVolatile[T]],
                                                   item: T): SeriesGrowVolatile[SeriesGrowVolatile[T]] = {
    val newSeriesList = SeriesGrowVolatile[SeriesGrowVolatile[T]](series.length + 1)
    series.iterator foreach newSeriesList.add

    val sizePerSlice = series.headOrNull.innerArrayLength

    val newSeries = SeriesGrowVolatile[T](sizePerSlice)
    newSeries add item

    newSeriesList add newSeries
    newSeriesList
  }

  def empty[T >: Null : ClassTag]: SeriesGrowable[T] =
    volatile[T](0)
}

class SeriesGrowable[T >: Null : ClassTag](@volatile private var series: SeriesGrowVolatile[SeriesGrowVolatile[T]],
                                           lengthPerSlice: Int) {

  private var _size = 0

  def get(index: Int): T = {
    val foundOrNull =
      if (index < lengthPerSlice) {
        series
          .get(0)
          .get(index)

      } else {
        val listIndex = index / lengthPerSlice
        val indexInArray = index - (lengthPerSlice * listIndex)

        series
          .get(listIndex)
          .get(indexInArray)
      }

    if (foundOrNull == null)
      throw new ArrayIndexOutOfBoundsException(index)
    else
      foundOrNull
  }

  def add(item: T): Unit = {
    val last = series.lastOrNull
    if (last.isFull)
      series = SeriesGrowable.extend(series, item)
    else
      last.add(item)

    _size += 1
  }

  def lastOrNull: T =
    series.lastOrNull.lastOrNull

  def headOrNull: T = {
    val headOrNull = series.headOrNull
    if (headOrNull == null)
      null.asInstanceOf[T]
    else
      headOrNull.headOrNull
  }

  def size: Int =
    _size

  def findReverse[R >: T](from: Int, result: R)(f: T => Boolean): R = {
    var start = from

    while (start >= 0) {
      val next = get(start)
      if (f(next))
        return next

      start -= 1
    }

    result
  }

  def find[R >: T](from: Int, result: R)(f: T => Boolean): R = {
    var start = from

    while (start < _size) {
      val next = get(start)
      if (f(next))
        return next

      start += 1
    }

    result
  }

  def foreach(from: Int)(f: T => Unit): Unit = {
    var start = from

    while (start < _size) {
      f(get(start))
      start += 1
    }
  }

  def iteratorFlatten: Iterator[T] =
    new Iterator[T] {
      val iterators =
        series
          .iterator
          .flatMap(_.iterator)

      override def hasNext: Boolean =
        iterators.hasNext

      override def next(): T =
        iterators.next()
    }
}
