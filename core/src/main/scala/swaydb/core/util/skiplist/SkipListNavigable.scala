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
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.core.util.skiplist

import java.util
import java.util.Map
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.{BiConsumer, Consumer}

import swaydb.core.util.NullOps
import swaydb.data.slice.Slice

import scala.annotation.tailrec
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

private[core] abstract class SkipListNavigable[OK, OV, K <: OK, V <: OV] private(protected val sizer: AtomicInteger) extends SkipList[OK, OV, K, V] {

  protected def state: NavigableSkipListState[K, V, util.NavigableMap[K, V], util.Map[K, V]]

  def this(int: Int) {
    this(new AtomicInteger(int))
  }

  override def get(key: K): OV =
    toOptionValue(state.skipList.get(key))

  override def remove(key: K): Unit =
    if (state.skipList.remove(key) != null)
      sizer.decrementAndGet()

  override def put(key: K, value: V): Unit =
    if (state.skipList.put(key, value) == null)
      sizer.incrementAndGet()

  override def putIfAbsent(key: K, value: V): Boolean = {
    val added = state.skipList.putIfAbsent(key, value) == null
    if (added) sizer.incrementAndGet()
    added
  }

  def subMap(from: K, fromInclusive: Boolean, to: K, toInclusive: Boolean): Iterable[(K, V)] =
    state.skipList.subMap(from, fromInclusive, to, toInclusive).asScala

  override def floor(key: K): OV =
    toOptionValue(state.skipList.floorEntry(key))

  override def floorKeyValue(key: K): Option[(K, V)] =
    toOptionKeyValue(state.skipList.floorEntry(key))

  override def higher(key: K): OV =
    toOptionValue(state.skipList.higherEntry(key))

  override def higherKeyValue(key: K): Option[(K, V)] =
    toOptionKeyValue(state.skipList.higherEntry(key))

  override def ceiling(key: K): OV =
    toOptionValue(state.skipList.ceilingEntry(key))

  def isEmpty: Boolean =
    state.skipList.isEmpty

  override def nonEmpty: Boolean =
    !isEmpty

  override def clear(): Unit = {
    state.skipList.clear()
    sizer.set(0)
  }

  def contains(key: K): Boolean =
    state.skipList.containsKey(key)

  def headKey: OK =
    tryOptionKey(state.skipList.firstKey())

  def headKeyOrNull: K =
    NullOps.tryOrNull(state.skipList.firstKey()).asInstanceOf[K]

  def pollLastEntry(): Map.Entry[K, V] = {
    val entry = state.skipList.pollLastEntry()
    if (entry != null) sizer.decrementAndGet()
    entry
  }

  def pollFirstEntry(): Map.Entry[K, V] = {
    val entry = state.skipList.pollFirstEntry()
    if (entry != null) sizer.decrementAndGet()
    entry
  }

  def size =
    sizer.get()

  def headKeyValue: Option[(K, V)] =
    tryOptionKeyValue(state.skipList.firstEntry())

  def lastKeyValue: Option[(K, V)] =
    tryOptionKeyValue(state.skipList.lastEntry())

  def lastKey: OK =
    tryOptionKey(state.skipList.lastKey())

  def lastKeyOrNull: K =
    NullOps.tryOrNull(state.skipList.lastKey()).asInstanceOf[K]

  def ceilingKey(key: K): OK =
    toOptionKey(state.skipList.ceilingKey(key))

  def higherKey(key: K): OK =
    toOptionKey(state.skipList.higherKey(key))

  def lower(key: K): OV =
    toOptionValue(state.skipList.lowerEntry(key))

  def lowerKey(key: K): OK =
    toOptionKey(state.skipList.lowerKey(key))

  def count() =
    state.skipList.size()

  def last(): OV =
    toOptionValue(state.skipList.lastEntry())

  def head(): OV =
    toOptionValue(state.skipList.firstEntry())

  def values(): Iterable[V] =
    state.skipList.values().asScala

  def keys(): util.NavigableSet[K] =
    state.skipList.navigableKeySet()

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
    state.skipList.forEach {
      new BiConsumer[K, V] {
        override def accept(key: K, value: V): Unit =
          result = f(result, (key, value))
      }
    }
    result
  }

  def foreach[R](f: (K, V) => R): Unit =
    state.skipList.forEach {
      new BiConsumer[K, V] {
        override def accept(key: K, value: V): Unit =
          f(key, value)
      }
    }

  def toSlice[V2 >: V : ClassTag](size: Int): Slice[V2] = {
    val slice = Slice.of[V2](size)
    state.skipList.values() forEach {
      new Consumer[V] {
        def accept(keyValue: V): Unit =
          slice add keyValue
      }
    }
    slice
  }

  override def asScala: mutable.Map[K, V] =
    state.skipList.asScala
}
