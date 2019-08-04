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

import java.util
import java.util.concurrent.{ConcurrentNavigableMap, ConcurrentSkipListMap}
import java.util.function.BiConsumer

import swaydb.IO
import swaydb.data.slice.Slice

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable

private[core] sealed trait SkipList[K, V] {
  def put(key: K, value: V): Unit
  def putIfAbsent(key: K, value: V): Boolean
  def get(key: K): Option[V]
  def remove(key: K): Unit
  def floor(key: K): Option[V]
  def higher(key: K): Option[V]
  def higherKeyValue(key: K): Option[(K, V)]
  def ceiling(key: K): Option[V]
  def isEmpty: Boolean
  def nonEmpty: Boolean
  def clear(): Unit
  def size: Int
  def contains(key: K): Boolean
  def firstKey: Option[K]
  def first: Option[(K, V)]
  def last: Option[(K, V)]
  def lastKey: Option[K]
  def ceilingKey(key: K): Option[K]
  def ceilingValue(key: K): Option[V]
  def higherValue(key: K): Option[V]
  def higherKey(key: K): Option[K]
  def lowerValue(key: K): Option[V]
  def lower(key: K): Option[V]
  def lowerKey(key: K): Option[K]
  def count(): Int
  def lastValue(): Option[V]
  def headValue(): Option[V]
  def head: Option[(K, V)]
  def values(): util.Collection[V]
  def keys(): util.NavigableSet[K]
  def take(count: Int): Slice[V]
  def foldLeft[R](r: R)(f: (R, (K, V)) => R): R
  def foreach[R](f: (K, V) => R): Unit
  def subMap(from: K, to: K): ConcurrentNavigableMap[K, V]
  def subMap(from: K, fromInclusive: Boolean, to: K, toInclusive: Boolean): ConcurrentNavigableMap[K, V]
  def asScala: mutable.Map[K, V]
}

private[core] object SkipList {
  @inline def toOptionValue[K, V](entry: java.util.Map.Entry[K, V]): Option[V] =
    if (entry == null)
      None
    else
      Option(entry.getValue)

  @inline def toOptionKeyValue[K, V](entry: java.util.Map.Entry[K, V]): Option[(K, V)] =
    if (entry == null)
      None
    else
      Option((entry.getKey, entry.getValue))

  @inline final def tryOptionValue[K, V](block: => java.util.Map.Entry[K, V]): Option[V] =
    try
      toOptionValue(block)
    catch {
      case _: Throwable =>
        None
    }

  @inline final def tryOptionKeyValue[K, V](block: => java.util.Map.Entry[K, V]): Option[(K, V)] =
    try
      toOptionKeyValue(block)
    catch {
      case _: Throwable =>
        None
    }

  def concurrent[K, V](implicit ordering: Ordering[K]): ConcurrentSkipList[K, V] =
    new ConcurrentSkipList[K, V](new ConcurrentSkipListMap[K, V](ordering))
}

private[core] class ConcurrentSkipList[K, V](skipList: ConcurrentSkipListMap[K, V]) extends SkipList[K, V] {

  import SkipList._

  override def get(key: K): Option[V] =
    Option(skipList.get(key))

  override def remove(key: K): Unit =
    skipList.remove(key)

  override def put(key: K, value: V): Unit =
    skipList.put(key, value)

  def subMap(from: K, to: K): ConcurrentNavigableMap[K, V] =
    skipList.subMap(from, to)

  def subMap(from: K, fromInclusive: Boolean, to: K, toInclusive: Boolean): ConcurrentNavigableMap[K, V] =
    skipList.subMap(from, fromInclusive, to, toInclusive)

  /**
   * @return true
   */
  override def putIfAbsent(key: K, value: V): Boolean =
    skipList.putIfAbsent(key, value) == null

  override def floor(key: K): Option[V] =
    toOptionValue(skipList.floorEntry(key))

  override def higher(key: K): Option[V] =
    toOptionValue(skipList.higherEntry(key))

  override def higherKeyValue(key: K): Option[(K, V)] =
    toOptionKeyValue(skipList.higherEntry(key))

  override def ceiling(key: K): Option[V] =
    toOptionValue(skipList.ceilingEntry(key))

  def isEmpty: Boolean =
    skipList.isEmpty

  override def nonEmpty: Boolean =
    !isEmpty

  override def clear(): Unit =
    skipList.clear()

  def size: Int =
    skipList.size()

  def contains(key: K): Boolean =
    skipList.containsKey(key)

  def firstKey: Option[K] =
    IO.tryOrNone(skipList.firstKey())

  def first: Option[(K, V)] =
    tryOptionKeyValue(skipList.firstEntry())

  def last: Option[(K, V)] =
    tryOptionKeyValue(skipList.lastEntry())

  def lastKey: Option[K] =
    IO.tryOrNone(skipList.lastKey())

  def ceilingKey(key: K): Option[K] =
    Option(skipList.ceilingKey(key))

  def ceilingValue(key: K): Option[V] =
    toOptionValue(skipList.ceilingEntry(key))

  def higherValue(key: K): Option[V] =
    toOptionValue(skipList.higherEntry(key))

  def higherKey(key: K): Option[K] =
    Option(skipList.higherKey(key))

  def lowerValue(key: K): Option[V] =
    toOptionValue(skipList.lowerEntry(key))

  def lower(key: K): Option[V] =
    toOptionValue(skipList.lowerEntry(key))

  def lowerKey(key: K): Option[K] =
    Option(skipList.lowerKey(key))

  def count() =
    skipList.size()

  def lastValue(): Option[V] =
    toOptionValue(skipList.lastEntry())

  def headValue(): Option[V] =
    toOptionValue(skipList.firstEntry())

  def head: Option[(K, V)] =
    toOptionKeyValue(skipList.firstEntry())

  def values(): util.Collection[V] =
    skipList.values()

  def keys(): util.NavigableSet[K] =
    skipList.keySet()

  def take(count: Int): Slice[V] = {
    val slice = Slice.create(count)

    @tailrec
    def doTake(nextOption: Option[(K, V)]): Slice[V] =
      if (slice.isFull || nextOption.isEmpty)
        slice
      else {
        val (key, value) = nextOption.get
        slice add value
        doTake(higherKeyValue(key))
      }

    doTake(head).close()
  }

  def foldLeft[R](r: R)(f: (R, (K, V)) => R): R = {
    var result = r
    skipList.forEach {
      new BiConsumer[K, V] {
        override def accept(key: K, value: V): Unit =
          result = f(result, (key, value))
      }
    }
    result
  }

  def foreach[R](f: (K, V) => R): Unit =
    skipList.forEach {
      new BiConsumer[K, V] {
        override def accept(key: K, value: V): Unit =
          f(key, value)
      }
    }

  override def asScala: mutable.Map[K, V] =
    skipList.asScala
}

