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

import java.util.concurrent.ConcurrentHashMap

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.util.series.growable.SeriesGrowable
import swaydb.data.OptimiseWrites
import swaydb.data.order.KeyOrder

import scala.annotation.unchecked.uncheckedVariance
import scala.collection.mutable.ListBuffer

private[skiplist] sealed trait KeyValue[+K, +V]

private[skiplist] case object KeyValue {

  case object None extends KeyValue[Nothing, Nothing] {
  }

  object Some {
    @inline def apply[K, V](key: K, value: V, index: Int): Some[K, V] =
      new Some(key, value, index)
  }

  class Some[+K, +V](@volatile var key: K@uncheckedVariance = null.asInstanceOf[K],
                     @volatile var value: V@uncheckedVariance = null.asInstanceOf[K],
                     val index: Int) extends KeyValue[K, V] {

    @inline def toTuple =
      (key, value)

    def copy(): KeyValue.Some[K, V] =
      new Some[K, V](
        key = key,
        value = value,
        index = index
      )
  }
}

private[core] object SkipListSeries {

  val randomWriteWarning =
    s"Performance warning! Random write inserted. ${OptimiseWrites.productPrefix}.${classOf[OptimiseWrites.SequentialOrder].getName} is not optimised for random writes. Consider using ${OptimiseWrites.productPrefix}.${OptimiseWrites.RandomOrder.productPrefix}"

  private[skiplist] class State[K, V](@volatile private[skiplist] var series: SeriesGrowable[KeyValue.Some[K, V]],
                                      val hashIndex: Option[java.util.Map[K, KeyValue.Some[K, V]]])


  def apply[OptionKey, OptionValue, Key <: OptionKey, Value <: OptionValue](lengthPerSeries: Int,
                                                                            enableHashIndex: Boolean,
                                                                            nullKey: OptionKey,
                                                                            nullValue: OptionValue)(implicit ordering: KeyOrder[Key]): SkipListSeries[OptionKey, OptionValue, Key, Value] =
    new SkipListSeries[OptionKey, OptionValue, Key, Value](
      state =
        new State(
          series = SeriesGrowable.volatile(lengthPerSeries),
          hashIndex = if (enableHashIndex) Some(new ConcurrentHashMap(lengthPerSeries)) else None
        ),
      nullKey = nullKey,
      nullValue = nullValue
    )

  private def get[K, V](target: K,
                        series: SeriesGrowable[KeyValue.Some[K, V]],
                        hashIndex: Option[java.util.Map[K, KeyValue.Some[K, V]]])(implicit ordering: KeyOrder[K]): KeyValue[K, V] = {
    hashIndex match {
      case scala.Some(hashIndex) =>
        val value = hashIndex.get(ordering.comparableKey(target))
        if (value == null)
          KeyValue.None
        else
          value

      case scala.None =>
        var start = 0
        var end = series.length - 1

        while (start <= end) {
          val mid = start + (end - start) / 2
          val found = series.get(mid)
          val compare = ordering.compare(found.key, target)
          if (compare == 0)
            if (found.value == null)
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
  }

  private def lower[K, V](target: K,
                          series: SeriesGrowable[KeyValue.Some[K, V]],
                          hashIndex: Option[java.util.Map[K, KeyValue.Some[K, V]]])(implicit ordering: KeyOrder[K]): KeyValue[K, V] = {
    var start = 0
    var end =
      hashIndex match {
        case scala.Some(hashIndex) =>
          val found = hashIndex.get(ordering.comparableKey(target))
          if (found == null)
            series.length - 1
          else
            return series.findReverse(found.index - 1, KeyValue.None)(_.value != null)

        case scala.None =>
          series.length - 1
      }

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
                          series: SeriesGrowable[KeyValue.Some[K, V]],
                          hashIndex: Option[java.util.Map[K, KeyValue.Some[K, V]]])(implicit ordering: KeyOrder[K]): KeyValue[K, V] = {
    var start = 0
    var end =
      hashIndex match {
        case scala.Some(hashIndex) =>
          val found = hashIndex.get(ordering.comparableKey(target))
          if (found == null)
            series.length - 1
          else
            return found

        case scala.None =>
          series.length - 1
      }

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
                           series: SeriesGrowable[KeyValue.Some[K, V]],
                           hashIndex: Option[java.util.Map[K, KeyValue.Some[K, V]]])(implicit ordering: KeyOrder[K]): KeyValue[K, V] = {
    var start =
      hashIndex match {
        case scala.Some(hashIndex) =>
          val found = hashIndex.get(ordering.comparableKey(target))
          if (found == null)
            0
          else
            return series.find(found.index + 1, KeyValue.None)(_.value != null)

        case scala.None =>
          0
      }

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
                            series: SeriesGrowable[KeyValue.Some[K, V]],
                            hashIndex: Option[java.util.Map[K, KeyValue.Some[K, V]]])(implicit ordering: KeyOrder[K]): KeyValue[K, V] = {
    var start =
      hashIndex match {
        case scala.Some(hashIndex) =>
          val found = hashIndex.get(ordering.comparableKey(target))
          if (found == null)
            0
          else
            return found

        case scala.None =>
          0
      }

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
 * SkipList optimised for sequential writes.
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
private[core] class SkipListSeries[OK, OV, K <: OK, V <: OV] private(@volatile private[skiplist] var state: SkipListSeries.State[K, V],
                                                                     val nullKey: OK,
                                                                     val nullValue: OV)(implicit val keyOrder: KeyOrder[K]) extends SkipListBatchable[OK, OV, K, V] with SkipList[OK, OV, K, V] with LazyLogging { self =>

  private def iterator(): Iterator[KeyValue.Some[K, V]] =
    new Iterator[KeyValue.Some[K, V]] {
      var nextOne: KeyValue.Some[K, V] = _
      val sliceIterator = self.state.series.iteratorFlatten

      override def hasNext: Boolean =
        if (sliceIterator.hasNext) {
          nextOne = sliceIterator.next()
          while (nextOne.value == null && sliceIterator.hasNext)
            nextOne = sliceIterator.next()

          nextOne.value != null
        } else {
          false
        }

      override def next(): KeyValue.Some[K, V] =
        nextOne
    }

  @inline private def putRandom(key: K, value: V): Unit =
    SkipListSeries.get(key, state.series, state.hashIndex) match {
      case some: KeyValue.Some[K, V] =>
        some.value = value

      case KeyValue.None =>
        logger.warn(SkipListSeries.randomWriteWarning)
        //cannot overwrite an existing value. This is a random insert, start a new series!
        val newSeries =
          SkipListSeries[OK, OV, K, V](
            lengthPerSeries = state.series.length + 1, //1+ for the new entry just in-case this was the last entry.
            enableHashIndex = state.hashIndex.isDefined,
            nullKey = nullKey,
            nullValue = nullValue
          )

        var putSuccessful = false

        state.series.foreach(from = 0) {
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

        this.state = newSeries.state
    }

  @inline private def putSequential(keyValue: KeyValue.Some[K, V]) = {
    state.series add keyValue
    state.hashIndex foreach (_.put(keyOrder.comparableKey(keyValue.key), keyValue))
  }

  override def put(key: K, value: V): Unit = {
    val lastOrNull = state.series.lastOrNull
    if (lastOrNull == null)
      putSequential(KeyValue.Some(key, value, state.series.length))
    else if (keyOrder.gt(key, lastOrNull.key))
      putSequential(KeyValue.Some(key, value, state.series.length))
    else
      putRandom(key, value)
  }

  override def putIfAbsent(key: K, value: V): Boolean =
    SkipListSeries.get(key, state.series, state.hashIndex) match {
      case KeyValue.None =>
        put(key, value)
        true

      case _: KeyValue.Some[K, V] =>
        false
    }

  override def get(target: K): OV =
    SkipListSeries.get(target, state.series, state.hashIndex) match {
      case KeyValue.None =>
        nullValue

      case some: KeyValue.Some[K, V] =>
        some.value
    }

  override def remove(key: K): Unit =
    SkipListSeries.get(key, state.series, state.hashIndex) match {
      case some: KeyValue.Some[K, V] =>
        some.value = null.asInstanceOf[V]
        state.hashIndex.foreach(_.remove(keyOrder.comparableKey(key)))

      case KeyValue.None =>
    }

  override def lower(key: K): OV =
    SkipListSeries.lower(key, state.series, state.hashIndex) match {
      case KeyValue.None =>
        nullValue

      case some: KeyValue.Some[K, V] =>
        some.value
    }

  override def lowerKey(key: K): OK =
    SkipListSeries.lower(key, state.series, state.hashIndex) match {
      case KeyValue.None =>
        nullKey

      case some: KeyValue.Some[K, V] =>
        some.key
    }

  override def floor(key: K): OV =
    SkipListSeries.floor(key, state.series, state.hashIndex) match {
      case KeyValue.None =>
        nullValue

      case some: KeyValue.Some[K, V] =>
        some.value
    }

  override def floorKeyValue(key: K): Option[(K, V)] =
    SkipListSeries.floor(key, state.series, state.hashIndex) match {
      case KeyValue.None =>
        scala.None

      case some: KeyValue.Some[K, V] =>
        scala.Some(some.toTuple)
    }

  override def higher(key: K): OV =
    SkipListSeries.higher(key, state.series, state.hashIndex) match {
      case KeyValue.None =>
        nullValue

      case some: KeyValue.Some[K, V] =>
        some.value
    }

  override def higherKey(key: K): OK =
    SkipListSeries.higher(key, state.series, state.hashIndex) match {
      case KeyValue.None =>
        nullKey

      case some: KeyValue.Some[K, V] =>
        some.key
    }

  override def higherKeyValue(key: K): Option[(K, V)] =
    SkipListSeries.higher(key, state.series, state.hashIndex) match {
      case KeyValue.None =>
        scala.None

      case some: KeyValue.Some[K, V] =>
        scala.Some(some.toTuple)
    }

  override def ceiling(key: K): OV =
    SkipListSeries.ceiling(key, state.series, state.hashIndex) match {
      case KeyValue.None =>
        nullValue

      case some: KeyValue.Some[K, V] =>
        some.value
    }

  override def ceilingKey(key: K): OK =
    SkipListSeries.ceiling(key, state.series, state.hashIndex) match {
      case KeyValue.None =>
        nullKey

      case some: KeyValue.Some[K, V] =>
        some.key
    }

  override def isEmpty: Boolean =
    state.hashIndex match {
      case scala.Some(value) =>
        value.isEmpty

      case scala.None =>
        iterator().isEmpty
    }

  override def nonEmpty: Boolean =
    !isEmpty

  override def clear(): Unit = {
    state.series = SeriesGrowable.empty
    state.hashIndex.foreach(_.clear())
  }

  override def size: Int =
    state.series.length

  override def contains(key: K): Boolean =
    SkipListSeries.get(key, state.series, state.hashIndex) match {
      case KeyValue.None =>
        false

      case some: KeyValue.Some[_, _] =>
        true
    }

  private def headOrNullSome(): KeyValue.Some[K, V] = {
    val head = state.series.headOrNull
    if (head == null)
      null
    else if (head.value == null)
      state.series.find(0, null)(_.value != null)
    else
      head
  }

  private def lastOrNullSome(): KeyValue.Some[K, V] = {
    val last = state.series.lastOrNull
    if (last == null)
      null
    else if (last.value == null)
      state.series.findReverse(state.series.length - 1, null)(_.value != null)
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
    iterator().size

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
        self.iterator().map(_.value)
    }

  override def foldLeft[R](r: R)(f: (R, (K, V)) => R): R =
    iterator().foldLeft(r) {
      case (r, some) =>
        f(r, (some.key, some.value))
    }

  override def foreach[R](f: (K, V) => R): Unit =
    iterator() foreach {
      keyValue =>
        f(keyValue.key, keyValue.value)
    }

  override def asScala: Iterable[(K, V)] =
    new Iterable[(K, V)] {
      override def iterator: Iterator[(K, V)] =
        self.iterator().map(_.toTuple)
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
          SkipListSeries.ceiling(from, state.series, state.hashIndex)
        else
          SkipListSeries.higher(from, state.series, state.hashIndex)

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
          SkipListSeries.floor(to, state.series, state.hashIndex)
        else
          SkipListSeries.lower(to, state.series, state.hashIndex)

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
        state.series.foldLeft(fromFound.index, toFound.index, ListBuffer.empty[(K, V)]) {
          case (buffer, keyValue) =>
            buffer += ((keyValue.key, keyValue.value))
        }
    }
  }

  override def batch(batches: Iterable[SkipList.Batch[K, V]]): Unit =
    if (batches.size > 1) {
      logger.warn(SkipListSeries.randomWriteWarning)

      val newSkipList =
        SkipListSeries[OK, OV, K, V](
          lengthPerSeries = this.size + batches.size,
          enableHashIndex = this.state.hashIndex.isDefined,
          nullKey = this.nullKey,
          nullValue = this.nullValue
        )

      val sortedBatches = batches.toArray.sortBy(_.key)(keyOrder)
      var batchIndex = 0

      state.series.foreach(0) {
        keyValue =>
          if (batchIndex < sortedBatches.length && keyOrder.gt(sortedBatches(batchIndex).key, keyValue.key)) {
            sortedBatches(batchIndex) apply newSkipList
            batchIndex += 1
          }

          newSkipList.put(keyValue.key, keyValue.value)
      }

      this.state = newSkipList.state
    } else {

      batches foreach {
        batch =>
          batch apply self
      }
    }
}
