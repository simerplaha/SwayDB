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

package swaydb.core.series.growable

import swaydb.core.series.appendable.SeriesAppendableVolatile

import scala.reflect.ClassTag

private[swaydb] object SeriesGrowableList {

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

private[swaydb] class SeriesGrowableList[T >: Null : ClassTag] private(@volatile private var series: SeriesAppendableVolatile[SeriesAppendableVolatile[T]],
                                                                       lengthPerSeries: Int) { self =>

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

  def iterable: Iterable[T] =
    new Iterable[T] {
      override def iterator: Iterator[T] =
        self.iterator
    }

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
