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

import swaydb.core.util.series.appendable.SeriesAppendableVolatile

import scala.reflect.ClassTag

private[core] object SeriesGrowableList {

  @inline def volatile[T >: Null : ClassTag](lengthPerSeries: Int): SeriesGrowableList[T] = {
    val sizePerSeries = lengthPerSeries max 1 //size cannot be 0

    val series = SeriesAppendableVolatile[SeriesAppendableVolatile[T]](1)
    val items = SeriesAppendableVolatile[T](sizePerSeries)
    series.add(items)

    new SeriesGrowableList(
      series = series,
      lengthPerSeries = sizePerSeries
    )
  }

  def empty[T >: Null : ClassTag]: SeriesGrowableList[T] =
    volatile[T](0)

  @inline private def extend[T >: Null : ClassTag](series: SeriesAppendableVolatile[SeriesAppendableVolatile[T]],
                                                   item: T): SeriesAppendableVolatile[SeriesAppendableVolatile[T]] = {
    val newSeriesList = SeriesAppendableVolatile[SeriesAppendableVolatile[T]](series.length + 1)
    series.iterator foreach newSeriesList.add

    val sizePerSlice = series.headOrNull.innerArrayLength

    val newSeries = SeriesAppendableVolatile[T](sizePerSlice)
    newSeries add item

    newSeriesList add newSeries
    newSeriesList
  }

}

private[core] class SeriesGrowableList[T >: Null : ClassTag] private(@volatile private var series: SeriesAppendableVolatile[SeriesAppendableVolatile[T]],
                                                                     lengthPerSeries: Int) {

  @volatile private var _written = 0

  def add(item: T): Unit = {
    val last = series.lastOrNull
    if (last.isFull)
      series = SeriesGrowableList.extend(series, item)
    else
      last.add(item)

    _written += 1
  }

  def get(index: Int): T = {
    val foundOrNull =
      if (index < lengthPerSeries) {
        series
          .get(0)
          .get(index)

      } else {
        val blockIndex = index / lengthPerSeries
        val arrayIndex = index - (lengthPerSeries * blockIndex)

        series
          .get(blockIndex)
          .get(arrayIndex)
      }

    if (foundOrNull == null)
      throw new ArrayIndexOutOfBoundsException(index)
    else
      foundOrNull
  }


  //parent series is always non-empty
  def lastOrNull: T =
    series.lastOrNull.lastOrNull

  //parent series is always non-empty
  def headOrNull: T =
    series.headOrNull.headOrNull

  def length: Int =
    _written

  def findReverse[R >: T](from: Int, nonResult: R)(f: T => Boolean): R = {
    var start = from

    while (start >= 0) {
      val next = get(start)
      if (f(next))
        return next

      start -= 1
    }

    nonResult
  }

  def find[R >: T](from: Int, nonResult: R)(f: T => Boolean): R = {
    var start = from

    while (start < _written) {
      val next = get(start)
      if (f(next))
        return next

      start += 1
    }

    nonResult
  }

  def foreach(from: Int)(f: T => Unit): Unit = {
    var index = from

    while (index < _written) {
      f(get(index))
      index += 1
    }
  }

  def foreach(from: Int, to: Int)(f: T => Unit): Unit = {
    var index = from

    while (index < _written && index <= to) {
      f(get(index))
      index += 1
    }
  }

  def foldLeft[R](from: Int, r: R)(f: (R, T) => R): R = {
    var result = r

    foreach(from) {
      item =>
        result = f(result, item)
    }

    result
  }

  def foldLeft[R](from: Int, to: Int, r: R)(f: (R, T) => R): R = {
    var result = r

    foreach(from, to) {
      item =>
        result = f(result, item)
    }

    result
  }

  def depth =
    series.length

  def isEmpty: Boolean =
    _written == 0

  def nonEmpty: Boolean =
    !isEmpty

  def iterator: Iterator[T] =
    new Iterator[T] {
      var index = 0
      var item: T = _

      override def hasNext: Boolean = {
        if (index < _written) {
          item = get(index)
          index += 1
          item != null
        } else {
          false
        }
      }

      override def next(): T =
        item
    }
}
