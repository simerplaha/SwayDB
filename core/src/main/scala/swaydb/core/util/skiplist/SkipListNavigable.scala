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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.core.util.skiplist

import java.util
import java.util.Map
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.{BiConsumer, Consumer}

import swaydb.core.util.NullOps
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice

import scala.annotation.tailrec
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

private[core] abstract class SkipListNavigable[OK, OV, K <: OK, V <: OV] private(protected val sizer: AtomicInteger) extends SkipList[OK, OV, K, V] {

  protected def skipList: util.NavigableMap[K, V]

  def this(int: Int)(implicit ordering: KeyOrder[K]) {
    this(new AtomicInteger(int))
  }

  override def get(key: K): OV =
    toOptionValue(skipList.get(key))

  override def remove(key: K): Unit =
    if (skipList.remove(key) != null)
      sizer.decrementAndGet()

  override def put(key: K, value: V): Unit =
    if (skipList.put(key, value) == null)
      sizer.incrementAndGet()

  override def putIfAbsent(key: K, value: V): Boolean = {
    val added = skipList.putIfAbsent(key, value) == null
    if (added) {
      sizer.incrementAndGet()
      skipList.put(key, value)
    }
    added
  }

  def subMap(from: K, fromInclusive: Boolean, to: K, toInclusive: Boolean): Iterable[(K, V)] =
    skipList.subMap(from, fromInclusive, to, toInclusive).asScala

  def subMapValues(from: K, fromInclusive: Boolean, to: K, toInclusive: Boolean): Iterable[V] =
    skipList.subMap(from, fromInclusive, to, toInclusive).values().asScala

  override def floor(key: K): OV =
    toOptionValue(skipList.floorEntry(key))

  override def floorKeyValue(key: K): Option[(K, V)] =
    toOptionKeyValue(skipList.floorEntry(key))

  override def higher(key: K): OV =
    toOptionValue(skipList.higherEntry(key))

  override def higherKeyValue(key: K): Option[(K, V)] =
    toOptionKeyValue(skipList.higherEntry(key))

  override def ceiling(key: K): OV =
    toOptionValue(skipList.ceilingEntry(key))

  def isEmpty: Boolean =
    skipList.isEmpty

  override def nonEmpty: Boolean =
    !isEmpty

  override def clear(): Unit = {
    skipList.clear()
    sizer.set(0)
  }

  def contains(key: K): Boolean =
    skipList.containsKey(key)

  def headKey: OK =
    tryOptionKey(skipList.firstKey())

  def headKeyOrNull: K =
    NullOps.tryOrNull(skipList.firstKey()).asInstanceOf[K]

  def pollLastEntry(): Map.Entry[K, V] = {
    val entry = skipList.pollLastEntry()
    if (entry != null) sizer.decrementAndGet()
    entry
  }

  def pollFirstEntry(): Map.Entry[K, V] = {
    val entry = skipList.pollFirstEntry()
    if (entry != null) sizer.decrementAndGet()
    entry
  }

  def size =
    sizer.get()

  def headKeyValue: Option[(K, V)] =
    tryOptionKeyValue(skipList.firstEntry())

  def lastKeyValue: Option[(K, V)] =
    tryOptionKeyValue(skipList.lastEntry())

  def lastKey: OK =
    tryOptionKey(skipList.lastKey())

  def lastKeyOrNull: K =
    NullOps.tryOrNull(skipList.lastKey()).asInstanceOf[K]

  def ceilingKey(key: K): OK =
    toOptionKey(skipList.ceilingKey(key))

  def higherKey(key: K): OK =
    toOptionKey(skipList.higherKey(key))

  def lower(key: K): OV =
    toOptionValue(skipList.lowerEntry(key))

  def lowerKey(key: K): OK =
    toOptionKey(skipList.lowerKey(key))

  def count() =
    skipList.size()

  def last(): OV =
    toOptionValue(skipList.lastEntry())

  def head(): OV =
    toOptionValue(skipList.firstEntry())

  def values(): Iterable[V] =
    skipList.values().asScala

  def keys(): util.NavigableSet[K] =
    skipList.navigableKeySet()

  def take(count: Int)(implicit classTag: ClassTag[V]): Slice[V] = {
    val slice = Slice.of[V](count)

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

  def toSlice[V2 >: V : ClassTag](size: Int): Slice[V2] = {
    val slice = Slice.of[V2](size)
    skipList.values() forEach {
      new Consumer[V] {
        def accept(keyValue: V): Unit =
          slice add keyValue
      }
    }
    slice
  }

  override def toIterable: mutable.Map[K, V] =
    skipList.asScala

  override def iterator: Iterator[(K, V)] =
    skipList.asScala.iterator

  override def valuesIterator: Iterator[V] =
    skipList.values().iterator().asScala

}
