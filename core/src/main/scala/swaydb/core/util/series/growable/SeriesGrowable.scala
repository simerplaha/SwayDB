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

import swaydb.core.util.Walker
import swaydb.core.util.series.appendable.SeriesAppendableVolatile

import scala.reflect.ClassTag

private[core] object SeriesGrowable {

  class State[T >: Null](@volatile var startIndex: Int,
                         @volatile var endIndex: Int,
                         val series: SeriesAppendableVolatile[SeriesAppendableVolatile[T]]) {
    def length =
      if (endIndex < 0)
        0
      else
        (endIndex - startIndex) + 1
  }

  @inline def volatile[T >: Null : ClassTag](lengthPerSeries: Int): SeriesGrowable[T] = {
    val sizePerSeries = lengthPerSeries max 1 //size cannot be 0

    val series = SeriesAppendableVolatile[SeriesAppendableVolatile[T]](1)
    val items = SeriesAppendableVolatile[T](sizePerSeries)
    series.add(items)

    val state = new State[T](0, -1, series)

    new SeriesGrowable(
      state = state,
      lengthPerSeries = sizePerSeries
    )
  }

  def empty[T >: Null : ClassTag]: SeriesGrowable[T] =
    volatile[T](0)

  @inline private def extend[T >: Null : ClassTag](item: T, state: State[T]): State[T] = {
    val sizePerSlice = state.series.headOrNull.innerArrayLength
    //if head array is remove drop it from new state
    if (state.startIndex >= state.series.headOrNull.length) {
      //no need to increment array since the first array is dropped.
      val newSeriesList = SeriesAppendableVolatile[SeriesAppendableVolatile[T]](state.series.length)

      state.series.iterator(1) foreach newSeriesList.add

      val extended = SeriesAppendableVolatile[T](sizePerSlice)
      newSeriesList add extended

      extended add item

      new State[T](
        startIndex = state.startIndex - state.series.headOrNull.length,
        endIndex = state.endIndex - state.series.headOrNull.length + 1,
        series = newSeriesList
      )
    } else {
      val newSeriesList = SeriesAppendableVolatile[SeriesAppendableVolatile[T]](state.series.length + 1)

      //head is not remove yet so just extend the Array.
      state.series.iterator foreach newSeriesList.add

      val extended = SeriesAppendableVolatile[T](sizePerSlice)
      newSeriesList add extended

      extended add item

      new State[T](
        startIndex = state.startIndex,
        endIndex = state.endIndex + 1,
        series = newSeriesList
      )
    }
  }

}

private[core] class SeriesGrowable[T >: Null : ClassTag] private(@volatile private var state: SeriesGrowable.State[T],
                                                                 lengthPerSeries: Int) { self =>

  //We don't not concurrently do add and remove.
  //They occur sequentially by a single thread. Compaction can concurrently invoke
  //removeHead and the cost of remove is just incrementing a value which is very low.
  def add(item: T): Unit =
    this.synchronized {
      val last = state.series.lastOrNull

      if (last.isFull) {
        state = SeriesGrowable.extend(item, state)
      } else {
        last.add(item)
        state.endIndex += 1
      }
    }

  def removeHead(): Boolean =
    this.synchronized {
      val canRemove = state.length > 0
      if (canRemove) state.startIndex += 1
      canRemove
    }

  def getOrNull(index: Int): T =
    try
      get(index)
    catch {
      case _: ArrayIndexOutOfBoundsException =>
        null
    }

  def get(index: Int): T = {
    val fromIndex = state.startIndex + index

    if (fromIndex > state.endIndex) {
      throw new ArrayIndexOutOfBoundsException(index)

    } else if (fromIndex < lengthPerSeries) {
      state
        .series
        .get(0)
        .get(fromIndex)

    } else {
      val blockIndex = fromIndex / lengthPerSeries
      val arrayIndex = fromIndex - (lengthPerSeries * blockIndex)

      state
        .series
        .get(blockIndex)
        .get(arrayIndex)
    }
  }

  def length: Int =
    state.length


  //parent series is always non-empty
  def headOrNull: T = {
    //prefetch length for cases where there are concurrent rmoved
    val len = length - 1
    if (len < 0)
      null
    else
      get(0)
  }

  //parent series is always non-empty
  def lastOrNull: T = {
    val len = length - 1
    if (len < 0)
      null
    else
      get(len)
  }

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

    while (start < length) {
      val next = get(start)
      if (f(next))
        return next

      start += 1
    }

    nonResult
  }

  def foreach(from: Int)(f: T => Unit): Unit = {
    var index = from

    while (index < length) {
      f(get(index))
      index += 1
    }
  }

  def foreach(from: Int, to: Int)(f: T => Unit): Unit = {
    var index = from

    while (index < length && index <= to) {
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
    state.series.length

  def isEmpty: Boolean =
    length == 0

  def nonEmpty: Boolean =
    !isEmpty

  def iterator: Iterator[T] =
    new Iterator[T] {
      var index = 0
      var item: T = _

      override def hasNext: Boolean = {
        item = getOrNull(index)
        index += 1
        item != null
      }

      override def next(): T =
        item
    }

  def reverseWalker(): Walker[T] =
    createReverseWalker(length - 1)

  def walker(): Walker[T] =
    createWalker(0)

  private def createWalker(index: Int): Walker[T] =
    new Walker[T] {

      override def headOrNull: T =
        self.getOrNull(index)

      override def dropHead(): Walker[T] =
        createWalker(index + 1)
    }

  private def createReverseWalker(index: Int): Walker[T] =
    new Walker[T] {

      override def headOrNull: T =
        if (index < 0)
          null
        else
          self.getOrNull(index)

      override def dropHead(): Walker[T] =
        createReverseWalker(index - 1)
    }
}
