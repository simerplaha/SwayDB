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

package swaydb.core.util.skiplist

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.util.series.growable.SeriesGrowable
import swaydb.core.util.{English, WhenOccurs}
import swaydb.data.OptimiseWrites
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice

import scala.collection.compat._
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

private[skiplist] sealed trait KeyValue[+K, +V]

private[skiplist] case object KeyValue {

  case object None extends KeyValue[Nothing, Nothing]

  /**
   * Stores key-value of for this SkipList. Being write optimised
   * this SkipList does not remove key-value but sets the value to
   * null indicating removed key-value. Removed key-value get omitted
   * from Iterators and other operations that search.
   */
  class Some[K, V](val key: K,
                   @volatile var value: V,
                   val index: Int) extends KeyValue[K, V] {

    @inline final def toTuple: (K, V) =
      (key, value)

    final def copy(): KeyValue.Some[K, V] =
      new Some[K, V](
        key = key,
        value = value,
        index = index
      )
  }
}

private[core] object SkipListSeries {

  def randomWriteWarning(count: Int) =
    s"Performance warning! Random write inserted $count ${English.plural(count, "time")}. " +
      s"${OptimiseWrites.productPrefix}.${classOf[OptimiseWrites.SequentialOrder].getSimpleName} is not optimised for random writes. " +
      s"Consider using ${OptimiseWrites.productPrefix}.${classOf[OptimiseWrites.RandomOrder].getSimpleName}."

  def apply[OptionKey, OptionValue, Key <: OptionKey, Value <: OptionValue](lengthPerSeries: Int,
                                                                            nullKey: OptionKey,
                                                                            nullValue: OptionValue)(implicit ordering: KeyOrder[Key]): SkipListSeries[OptionKey, OptionValue, Key, Value] =
    new SkipListSeries[OptionKey, OptionValue, Key, Value](
      series = SeriesGrowable.volatile(lengthPerSeries),
      nullKey = nullKey,
      nullValue = nullValue
    )

  /**
   * @param keepNullValue if true will return the [[KeyValue]] if it's value is set to null (means removed).
   *                      This is needed for inserting a new key-value if it's existing value was previously
   *                      removed.
   */
  private def get[K, V](target: K,
                        series: SeriesGrowable[KeyValue.Some[K, V]],
                        keepNullValue: Boolean)(implicit ordering: KeyOrder[K]): KeyValue[K, V] = {
    var start = 0
    var end = series.length - 1

    while (start <= end) {
      val mid = start + (end - start) / 2
      val found = series.get(mid)
      val compare = ordering.compare(found.key, target)
      if (compare == 0)
        if (!keepNullValue && found.value == null)
          return KeyValue.None
        else
          return found
      else if (compare < 0)
        start = mid + 1
      else
        end = mid - 1
    }

    KeyValue.None
  }

  private def lower[K, V](target: K,
                          series: SeriesGrowable[KeyValue.Some[K, V]])(implicit ordering: KeyOrder[K]): KeyValue[K, V] = {
    var start = 0
    var end = series.length - 1

    while (start <= end) {
      val mid = start + (end - start) / 2
      val found = series.get(mid)
      val compare = ordering.compare(found.key, target)
      if (compare == 0)
        if (mid == 0)
          return KeyValue.None
        else
          return series.findReverse(mid - 1, KeyValue.None)(_.value != null)

      else if (compare < 0)
        start = mid + 1
      else
        end = mid - 1
    }

    if (start <= 0)
      KeyValue.None
    else
      series.findReverse(start - 1, KeyValue.None)(_.value != null)
  }

  private def floor[K, V](target: K,
                          series: SeriesGrowable[KeyValue.Some[K, V]])(implicit ordering: KeyOrder[K]): KeyValue[K, V] = {
    var start = 0
    var end = series.length - 1

    while (start <= end) {
      val mid = start + (end - start) / 2
      val found = series.get(mid)
      val compare = ordering.compare(found.key, target)
      if (compare == 0)
        if (found.value != null)
          return found
        else
          return series.findReverse(mid - 1, KeyValue.None)(_.value != null)

      else if (compare < 0)
        start = mid + 1
      else
        end = mid - 1
    }

    if (start <= 0)
      KeyValue.None
    else
      series.findReverse(start - 1, KeyValue.None)(_.value != null)
  }

  private def higher[K, V](target: K,
                           series: SeriesGrowable[KeyValue.Some[K, V]])(implicit ordering: KeyOrder[K]): KeyValue[K, V] = {
    var start = 0
    var end = series.length - 1

    while (start <= end) {
      val mid = start + (end - start) / 2
      val found = series.get(mid)
      val compare = ordering.compare(found.key, target)
      if (compare == 0)
        if (mid == series.length - 1)
          return KeyValue.None
        else
          return series.find(mid + 1, KeyValue.None)(_.value != null)

      else if (compare < 0)
        start = mid + 1
      else
        end = mid - 1
    }

    if (end >= series.length - 1)
      KeyValue.None
    else
      series.find(end + 1, KeyValue.None)(_.value != null)
  }

  private def ceiling[K, V](target: K,
                            series: SeriesGrowable[KeyValue.Some[K, V]])(implicit ordering: KeyOrder[K]): KeyValue[K, V] = {
    var start = 0
    var end = series.length - 1

    while (start <= end) {
      val mid = start + (end - start) / 2
      val found = series.get(mid)
      val compare = ordering.compare(found.key, target)
      if (compare == 0)
        if (found.value != null)
          return found
        else
          return series.find(mid + 1, KeyValue.None)(_.value != null)

      else if (compare < 0)
        start = mid + 1
      else
        end = mid - 1
    }

    if (end >= series.length - 1)
      KeyValue.None
    else
      series.find(end + 1, KeyValue.None)(_.value != null)
  }
}

/**
 * SkipList that is sequential write efficient. Used for Queue data-type.
 * Ranomly writes are expensive.
 *
 * Random writes to this skipList is expensive and requires a full clone.
 *
 * Benchmark PUT
 * ConcurrentSkipListMap       - 0.346891 seconds
 * SkipListSeries HashEnabled  - 0.272745 seconds
 * SkipListSeries HashDisabled - 0.232691 seconds
 *
 * Benchmark GET
 * ConcurrentSkipListMap       - 0.148196 seconds
 * SkipListSeries HashEnabled  - 0.055263 seconds
 * SkipListSeries HashDisabled - 0.229062 seconds
 *
 * Benchmark LOWER
 * ConcurrentSkipListMap       - 0.335982 seconds
 * SkipListSeries HashEnabled  - 0.069562 seconds
 * SkipListSeries HashDisabled - 0.254108 seconds
 *
 */
private[core] class SkipListSeries[OK, OV, K <: OK, V <: OV] private(@volatile private[skiplist] var series: SeriesGrowable[KeyValue.Some[K, V]],
                                                                     val nullKey: OK,
                                                                     val nullValue: OV)(implicit val keyOrder: KeyOrder[K]) extends SkipList[OK, OV, K, V] with LazyLogging { self =>

  private val randomWriteWarning = WhenOccurs(500)(times => logger.warn(SkipListSeries.randomWriteWarning(times)))

  private def iteratorKeyValue(): Iterator[KeyValue.Some[K, V]] =
    new Iterator[KeyValue.Some[K, V]] {
      var nextOne: KeyValue.Some[K, V] = null
      val sliceIterator = self.series.iterator

      override def hasNext: Boolean = {
        if (sliceIterator.hasNext) {
          nextOne = sliceIterator.next()
          while (nextOne.value == null && sliceIterator.hasNext)
            nextOne = sliceIterator.next()

          nextOne.value != null
        } else {
          false
        }
      }

      override def next(): KeyValue.Some[K, V] =
        nextOne
    }

  override def iterator: Iterator[(K, V)] =
    iteratorKeyValue().map(_.toTuple)

  override def valuesIterator: Iterator[V] =
    iteratorKeyValue().map(_.value)

  @inline private def putRandom(key: K, value: V): Unit =
    SkipListSeries.get(target = key, series = series, keepNullValue = true) match {
      case some: KeyValue.Some[K, V] =>
        some.value = value

      case KeyValue.None =>
        randomWriteWarning.occurs()
        //cannot overwrite an existing value. This is a random insert, start a new series!
        val newSeries =
          SkipListSeries[OK, OV, K, V](
            lengthPerSeries = series.length + 1, //1+ for the new entry just in-case this was the last entry.
            nullKey = nullKey,
            nullValue = nullValue
          )

        var putSuccessful = false

        series.foreach(from = 0) {
          existing =>
            if (putSuccessful) {
              newSeries.put(existing.key, existing.value)
            } else {
              val existingKeyCompare = keyOrder.compare(existing.key, key)
              if (existingKeyCompare < 0) {
                newSeries.put(existing.key, existing.value)
              } else if (existingKeyCompare > 0) {
                newSeries.put(key, value)
                newSeries.put(existing.key, existing.value)
                putSuccessful = true
              } else {
                //the above get which uses binarySearch and hashIndex should've already
                //handled cases where put key exists.
                throw new Exception("Get should updated this.")
              }
            }
        }

        this.series = newSeries.series
    }

  @inline private def putSequential(keyValue: KeyValue.Some[K, V]) =
    series add keyValue

  override def put(key: K, value: V): Unit = {
    val lastOrNull = series.lastOrNull
    if (lastOrNull == null)
      putSequential(new KeyValue.Some(key, value, series.length))
    else if (keyOrder.gt(key, lastOrNull.key))
      putSequential(new KeyValue.Some(key, value, series.length))
    else
      putRandom(key, value)
  }

  override def putIfAbsent(key: K, value: V): Boolean =
    SkipListSeries.get(key, series, keepNullValue = false) match {
      case KeyValue.None =>
        put(key, value)
        true

      case _: KeyValue.Some[K, V] =>
        false
    }

  override def get(target: K): OV =
    SkipListSeries.get(target, series, keepNullValue = false) match {
      case KeyValue.None =>
        nullValue

      case some: KeyValue.Some[K, V] =>
        some.value
    }

  override def remove(key: K): Unit =
    SkipListSeries.get(key, series, keepNullValue = false) match {
      case some: KeyValue.Some[K, V] =>
        //do not clear from HashIndex. Mutate this value is only needed for reads to properly
        //perform searches.
        some.value = null.asInstanceOf[V]

      case KeyValue.None =>
    }

  override def lower(key: K): OV =
    SkipListSeries.lower(key, series) match {
      case KeyValue.None =>
        nullValue

      case some: KeyValue.Some[K, V] =>
        some.value
    }

  override def lowerKey(key: K): OK =
    SkipListSeries.lower(key, series) match {
      case KeyValue.None =>
        nullKey

      case some: KeyValue.Some[K, V] =>
        some.key
    }

  override def floor(key: K): OV =
    SkipListSeries.floor(key, series) match {
      case KeyValue.None =>
        nullValue

      case some: KeyValue.Some[K, V] =>
        some.value
    }

  override def floorKeyValue(key: K): Option[(K, V)] =
    SkipListSeries.floor(key, series) match {
      case KeyValue.None =>
        scala.None

      case some: KeyValue.Some[K, V] =>
        scala.Some(some.toTuple)
    }

  override def higher(key: K): OV =
    SkipListSeries.higher(key, series) match {
      case KeyValue.None =>
        nullValue

      case some: KeyValue.Some[K, V] =>
        some.value
    }

  override def higherKey(key: K): OK =
    SkipListSeries.higher(key, series) match {
      case KeyValue.None =>
        nullKey

      case some: KeyValue.Some[K, V] =>
        some.key
    }

  override def higherKeyValue(key: K): Option[(K, V)] =
    SkipListSeries.higher(key, series) match {
      case KeyValue.None =>
        scala.None

      case some: KeyValue.Some[K, V] =>
        scala.Some(some.toTuple)
    }

  override def ceiling(key: K): OV =
    SkipListSeries.ceiling(key, series) match {
      case KeyValue.None =>
        nullValue

      case some: KeyValue.Some[K, V] =>
        some.value
    }

  override def ceilingKey(key: K): OK =
    SkipListSeries.ceiling(key, series) match {
      case KeyValue.None =>
        nullKey

      case some: KeyValue.Some[K, V] =>
        some.key
    }

  override def isEmpty: Boolean =
    iteratorKeyValue().isEmpty

  override def nonEmpty: Boolean =
    !isEmpty

  override def clear(): Unit =
    series = SeriesGrowable.empty

  override def size: Int =
    series.length

  override def contains(key: K): Boolean =
    SkipListSeries.get(key, series, keepNullValue = false) match {
      case KeyValue.None =>
        false

      case _: KeyValue.Some[_, _] =>
        true
    }

  private def headOrNullSome(): KeyValue.Some[K, V] = {
    val head = series.headOrNull
    if (head == null)
      null
    else if (head.value == null)
      series.find(0, null)(_.value != null)
    else
      head
  }

  private def lastOrNullSome(): KeyValue.Some[K, V] = {
    val last = series.lastOrNull
    if (last == null)
      null
    else if (last.value == null)
      series.findReverse(series.length - 1, null)(_.value != null)
    else
      last
  }

  override def headKey: OK = {
    val some = headOrNullSome()
    if (some == null)
      nullKey
    else
      some.key
  }

  override def lastKey: OK = {
    val some = lastOrNullSome()
    if (some == null)
      nullKey
    else
      some.key
  }

  override def count(): Int =
    iteratorKeyValue().size

  override def last(): OV = {
    val some = lastOrNullSome()
    if (some == null)
      nullValue
    else
      some.value
  }

  override def head(): OV = {
    val some = headOrNullSome()
    if (some == null)
      nullValue
    else
      some.value
  }

  override def headKeyValue: Option[(K, V)] = {
    val some = headOrNullSome()
    if (some == null)
      scala.None
    else
      scala.Some((some.key, some.value))
  }

  override def values(): Iterable[V] =
    new Iterable[V] {
      override def iterator: Iterator[V] =
        self.iteratorKeyValue().map(_.value)
    }

  override def foldLeft[R](r: R)(f: (R, (K, V)) => R): R =
    iteratorKeyValue().foldLeft(r) {
      case (r, some) =>
        f(r, (some.key, some.value))
    }

  override def foreach[R](f: (K, V) => R): Unit =
    iteratorKeyValue() foreach {
      keyValue =>
        f(keyValue.key, keyValue.value)
    }

  override def toIterable: Iterable[(K, V)] =
    self.iteratorKeyValue().map(_.toTuple).to(Iterable)

  def toValuesSlice()(implicit classTag: ClassTag[V]): Slice[V] = {
    val slice = Slice.of[V](self.size)

    //use iterator to clear all removed key-values (null values). SkipListSeries is write optimised backed
    //by an Array so it cannot physically remove the key-value which would require reordering of the
    //Arrays instead it nulls the value indicating remove.
    iteratorKeyValue() foreach {
      keyValue =>
        slice add keyValue.value
    }

    slice
  }

  override def subMap(from: K, fromInclusive: Boolean, to: K, toInclusive: Boolean): Iterable[(K, V)] = {
    val compare = keyOrder.compare(from, to)

    if (compare == 0) {
      if (fromInclusive && toInclusive) {
        val found = get(from)
        if (found == nullValue)
          Iterable.empty
        else
          Iterable((from, found.asInstanceOf[V]))
      } else {
        Iterable.empty
      }
    } else if (compare > 0) {
      //following Java's ConcurrentSkipListMap's Exception.
      throw new IllegalArgumentException("Invalid input. fromKey cannot be greater than toKey.")
    } else {
      val fromResult =
        if (fromInclusive)
          SkipListSeries.ceiling(from, series)
        else
          SkipListSeries.higher(from, series)

      val fromFound: KeyValue.Some[K, V] =
        fromResult match {
          case KeyValue.None =>
            //terminate early no need to run toSearch
            return Iterable.empty

          case some: KeyValue.Some[K, V] =>
            some
        }

      val toResult =
        if (toInclusive)
          SkipListSeries.floor(to, series)
        else
          SkipListSeries.lower(to, series)

      val toFound =
        toResult match {
          case KeyValue.None =>
            //terminate - no need to compare
            return Iterable.empty

          case some: KeyValue.Some[K, V] =>
            some
        }

      val compare = keyOrder.compare(fromFound.key, toFound.key)

      if (compare == 0)
        Iterable((fromFound.key, fromFound.value))
      else if (compare > 0)
        Iterable.empty
      else
        series.foldLeft(fromFound.index, toFound.index, ListBuffer.empty[(K, V)]) {
          case (buffer, keyValue) =>
            if (keyValue.value != null)
              buffer += ((keyValue.key, keyValue.value))
            else
              buffer
        }
    }
  }
}
