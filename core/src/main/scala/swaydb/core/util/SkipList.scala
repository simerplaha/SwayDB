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
import java.util.concurrent.ConcurrentSkipListMap
import java.util.function.BiConsumer

import swaydb.IO
import swaydb.data.slice.Slice

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.reflect.ClassTag

private[core] sealed trait SkipList[K, V] {
  def put(key: K, value: V): Unit
  def putIfAbsent(key: K, value: V): Boolean
  def get(key: K): Option[V]
  def remove(key: K): Unit
  def floor(key: K): Option[V]

  def higher(key: K): Option[V]
  def higherKey(key: K): Option[K]
  def higherKeyValue(key: K): Option[(K, V)]

  def ceiling(key: K): Option[V]
  def ceilingKey(key: K): Option[K]

  def isEmpty: Boolean
  def nonEmpty: Boolean
  def clear(): Unit
  def size: Int
  def contains(key: K): Boolean
  def headKey: Option[K]
  def lastKey: Option[K]

  def lower(key: K): Option[V]
  def lowerKey(key: K): Option[K]
  def count(): Int
  def last(): Option[V]
  def head(): Option[V]
  def headKeyValue: Option[(K, V)]
  def values(): util.Collection[V]
  def keys(): util.NavigableSet[K]
  def take(count: Int): Slice[V]
  def foldLeft[R](r: R)(f: (R, (K, V)) => R): R
  def foreach[R](f: (K, V) => R): Unit
  def subMap(from: K, to: K): util.NavigableMap[K, V]
  def subMap(from: K, fromInclusive: Boolean, to: K, toInclusive: Boolean): util.NavigableMap[K, V]
  def asScala: mutable.Map[K, V]
  def isConcurrent: Boolean
}

private[core] object SkipList {
  object KeyValue {
    def apply[K, V](key: K, value: V): KeyValue[K, V] =
      new KeyValue(key, value)
  }
  class KeyValue[K, V](val key: K, val value: V)

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

  def value[K, V: ClassTag](implicit ordering: Ordering[K]): SkipListValue[K, V] =
    new SkipListValue[K, V](None)

}

private[core] class ConcurrentSkipList[K, V](skipList: ConcurrentSkipListMap[K, V]) extends SkipList[K, V] {

  import SkipList._

  val isConcurrent: Boolean = true

  override def get(key: K): Option[V] =
    Option(skipList.get(key))

  override def remove(key: K): Unit =
    skipList.remove(key)

  override def put(key: K, value: V): Unit =
    skipList.put(key, value)

  def subMap(from: K, to: K): java.util.NavigableMap[K, V] =
    skipList.subMap(from, to)

  def subMap(from: K, fromInclusive: Boolean, to: K, toInclusive: Boolean): java.util.NavigableMap[K, V] =
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

  def headKey: Option[K] =
    IO.tryOrNone(skipList.firstKey())

  def headKeyValue: Option[(K, V)] =
    tryOptionKeyValue(skipList.firstEntry())

  def lastKeyValue: Option[(K, V)] =
    tryOptionKeyValue(skipList.lastEntry())

  def lastKey: Option[K] =
    IO.tryOrNone(skipList.lastKey())

  def ceilingKey(key: K): Option[K] =
    Option(skipList.ceilingKey(key))

  def higherKey(key: K): Option[K] =
    Option(skipList.higherKey(key))

  def lower(key: K): Option[V] =
    toOptionValue(skipList.lowerEntry(key))

  def lowerKey(key: K): Option[K] =
    Option(skipList.lowerKey(key))

  def count() =
    skipList.size()

  def last(): Option[V] =
    toOptionValue(skipList.lastEntry())

  def head(): Option[V] =
    toOptionValue(skipList.firstEntry())

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

    doTake(headKeyValue).close()
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

private[core] class SkipListValue[K, V: ClassTag](@volatile private var keyValue: Option[SkipList.KeyValue[K, V]])(implicit order: Ordering[K]) extends SkipList[K, V] {

  import order._

  val isConcurrent: Boolean = false

  override def put(key: K, value: V): Unit =
    keyValue = Some(SkipList.KeyValue(key, value))

  override def putIfAbsent(key: K, value: V): Boolean =
    if (keyValue.exists(_.key equiv key)) {
      false
    } else {
      put(key, value)
      true
    }

  override def get(key: K): Option[V] =
    keyValue flatMap {
      keyValue =>
        if (keyValue.key equiv key)
          Some(keyValue.value)
        else
          None
    }

  override def remove(key: K): Unit =
    keyValue foreach {
      keyValue =>
        if (keyValue.key equiv key)
          this.keyValue = None
    }

  override def floor(key: K): Option[V] =
    keyValue flatMap {
      keyValue =>
        if (keyValue.key <= key)
          Some(keyValue.value)
        else
          None
    }

  override def higher(key: K): Option[V] =
    keyValue flatMap {
      keyValue =>
        if (keyValue.key > key)
          Some(keyValue.value)
        else
          None
    }

  override def higherKeyValue(key: K): Option[(K, V)] =
    keyValue flatMap {
      keyValue =>
        if (keyValue.key > key)
          Some((keyValue.key, keyValue.value))
        else
          None
    }

  override def ceiling(key: K): Option[V] =
    keyValue flatMap {
      keyValue =>
        if (keyValue.key >= key)
          Some(keyValue.value)
        else
          None
    }

  override def isEmpty: Boolean =
    keyValue.isEmpty

  override def nonEmpty: Boolean =
    !isEmpty

  override def clear(): Unit =
    keyValue = None

  override def size: Int =
    if (isEmpty) 0 else 1

  override def contains(key: K): Boolean =
    keyValue.exists(_.key equiv key)

  override def headKey: Option[K] =
    keyValue.map(_.key)

  override def headKeyValue: Option[(K, V)] =
    keyValue.map(keyValue => (keyValue.key, keyValue.value))

  override def lastKey: Option[K] =
    headKey

  override def ceilingKey(key: K): Option[K] =
    keyValue flatMap {
      keyValue =>
        if (keyValue.key >= key)
          Some((keyValue.key))
        else
          None
    }

  override def higherKey(key: K): Option[K] =
    keyValue flatMap {
      keyValue =>
        if (keyValue.key > key)
          Some((keyValue.key))
        else
          None
    }

  override def lower(key: K): Option[V] =
    keyValue flatMap {
      keyValue =>
        if (keyValue.key < key)
          Some((keyValue.value))
        else
          None
    }

  override def lowerKey(key: K): Option[K] =
    keyValue flatMap {
      keyValue =>
        if (keyValue.key < key)
          Some((keyValue.key))
        else
          None
    }

  override def count(): Int =
    size

  override def last(): Option[V] =
    head()

  override def head(): Option[V] =
    keyValue.map(_.value)

  override def values(): util.Collection[V] = {
    val list = new util.ArrayList[V]()
    keyValue foreach {
      keyValue =>
        list.add(keyValue.value)
    }
    list
  }

  override def keys(): util.NavigableSet[K] = {
    val keySet = new java.util.TreeSet[K]()
    keyValue foreach {
      keyValue =>
        keySet.add(keyValue.key)
    }
    keySet
  }

  override def take(count: Int): Slice[V] =
    if (count <= 0)
      Slice.empty
    else
      keyValue.map(keyValue => Slice[V](keyValue.value)) getOrElse Slice.empty

  override def foldLeft[R](r: R)(f: (R, (K, V)) => R): R =
    keyValue.foldLeft(r) {
      case (r, keyValue) =>
        f(r, (keyValue.key, keyValue.value))
    }

  override def foreach[R](f: (K, V) => R): Unit =
    keyValue foreach {
      keyValue =>
        f(keyValue.key, keyValue.value)
    }

  override def subMap(from: K, to: K): util.NavigableMap[K, V] =
    subMap(
      from = from,
      fromInclusive = true,
      to = to,
      toInclusive = false
    )

  override def subMap(from: K, fromInclusive: Boolean, to: K, toInclusive: Boolean): util.NavigableMap[K, V] = {
    val map = new util.TreeMap[K, V]()
    keyValue foreach {
      keyValue =>
        if (((fromInclusive && keyValue.key >= from) || (!fromInclusive && keyValue.key > from)) && ((toInclusive && keyValue.key <= to) || (!toInclusive && keyValue.key < to)))
          map.put(keyValue.key, keyValue.value)
    }
    map
  }

  override def asScala: mutable.Map[K, V] = {
    val map = mutable.Map.empty[K, V]
    keyValue foreach {
      keyValue =>
        map.put(keyValue.key, keyValue.value)
    }
    map
  }
}
