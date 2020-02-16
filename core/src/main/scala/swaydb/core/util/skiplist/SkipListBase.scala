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
 */

package swaydb.core.util.skiplist

import java.util
import java.util.function.{BiConsumer, Consumer}

import swaydb.data.slice.Slice

import scala.annotation.tailrec
import scala.collection.mutable
import scala.reflect.ClassTag
import scala.jdk.CollectionConverters._

protected abstract class SkipListBase[OptionKey, OptionValue, Key <: OptionKey, Value <: OptionValue, SL <: util.NavigableMap[Key, Value]](@volatile var skipList: SL,
                                                                                                                                           val isConcurrent: Boolean) extends SkipList[OptionKey, OptionValue, Key, Value] {

  def cloneInstance(skipList: SL): SkipListBase[OptionKey, OptionValue, Key, Value, SL]

  override def get(key: Key): OptionValue =
    toOptionValue(skipList.get(key))

  override def remove(key: Key): Unit =
    skipList.remove(key)

  /**
   * Does not support concurrent batch writes since it's only being used by [[swaydb.core.level.Level]] which
   * write to appendix concurrently.
   */
  def batch(batches: Iterable[SkipList.Batch[Key, Value]]): Unit = {
    var cloned = false
    val targetSkipList =
      if (batches.size > 1) {
        cloned = true
        this.cloneInstance(skipList)
      } else {
        this
      }

    batches foreach {
      batch =>
        batch apply targetSkipList
    }

    if (cloned)
      this.skipList = targetSkipList.skipList
  }

  def put(keyValues: Iterable[(Key, Value)]): Unit = {
    var cloned = false
    val targetSkipList =
      if (keyValues.size > 1) {
        cloned = true
        this.cloneInstance(skipList).skipList
      } else {
        skipList
      }

    keyValues foreach {
      case (key, value) =>
        targetSkipList.put(key, value)
    }

    if (cloned)
      this.skipList = targetSkipList
  }

  override def put(key: Key, value: Value): Unit =
    skipList.put(key, value)

  def subMap(from: Key, to: Key): java.util.NavigableMap[Key, Value] =
    subMap(from = from, fromInclusive = true, to = to, toInclusive = false)

  def subMap(from: Key, fromInclusive: Boolean, to: Key, toInclusive: Boolean): java.util.NavigableMap[Key, Value] =
    skipList.subMap(from, fromInclusive, to, toInclusive)

  /**
   * @return true
   */
  override def putIfAbsent(key: Key, value: Value): Boolean =
    skipList.putIfAbsent(key, value) == null

  override def floor(key: Key): OptionValue =
    toOptionValue(skipList.floorEntry(key))

  override def floorKeyValue(key: Key): Option[(Key, Value)] =
    toOptionKeyValue(skipList.floorEntry(key))

  override def higher(key: Key): OptionValue =
    toOptionValue(skipList.higherEntry(key))

  override def higherKeyValue(key: Key): Option[(Key, Value)] =
    toOptionKeyValue(skipList.higherEntry(key))

  override def ceiling(key: Key): OptionValue =
    toOptionValue(skipList.ceilingEntry(key))

  def isEmpty: Boolean =
    skipList.isEmpty

  override def nonEmpty: Boolean =
    !isEmpty

  override def clear(): Unit =
    skipList.clear()

  def size: Int =
    skipList.size()

  def contains(key: Key): Boolean =
    skipList.containsKey(key)

  def headKey: OptionKey =
    tryOptionKey(skipList.firstKey())

  def headKeyValue: Option[(Key, Value)] =
    tryOptionKeyValue(skipList.firstEntry())

  def lastKeyValue: Option[(Key, Value)] =
    tryOptionKeyValue(skipList.lastEntry())

  def lastKey: OptionKey =
    tryOptionKey(skipList.lastKey())

  def ceilingKey(key: Key): OptionKey =
    toOptionKey(skipList.ceilingKey(key))

  def higherKey(key: Key): OptionKey =
    toOptionKey(skipList.higherKey(key))

  def lower(key: Key): OptionValue =
    toOptionValue(skipList.lowerEntry(key))

  def lowerKey(key: Key): OptionKey =
    toOptionKey(skipList.lowerKey(key))

  def count() =
    skipList.size()

  def last(): OptionValue =
    toOptionValue(skipList.lastEntry())

  def head(): OptionValue =
    toOptionValue(skipList.firstEntry())

  def values(): util.Collection[Value] =
    skipList.values()

  def keys(): util.NavigableSet[Key] =
    skipList.navigableKeySet()

  def take(count: Int): Slice[Value] = {
    val slice = Slice.create(count)

    @tailrec
    def doTake(nextOption: Option[(Key, Value)]): Slice[Value] =
      if (slice.isFull || nextOption.isEmpty)
        slice
      else {
        val (key, value) = nextOption.get
        slice add value
        doTake(higherKeyValue(key))
      }

    doTake(headKeyValue).close()
  }

  def foldLeft[R](r: R)(f: (R, (Key, Value)) => R): R = {
    var result = r
    skipList.forEach {
      new BiConsumer[Key, Value] {
        override def accept(key: Key, value: Value): Unit =
          result = f(result, (key, value))
      }
    }
    result
  }

  def foreach[R](f: (Key, Value) => R): Unit =
    skipList.forEach {
      new BiConsumer[Key, Value] {
        override def accept(key: Key, value: Value): Unit =
          f(key, value)
      }
    }

  def toSlice[V2 >: Value : ClassTag](size: Int): Slice[V2] = {
    val slice = Slice.create[V2](size)
    skipList.values() forEach {
      new Consumer[Value] {
        def accept(keyValue: Value): Unit =
          slice add keyValue
      }
    }
    slice
  }

  override def asScala: mutable.Map[Key, Value] =
    skipList.asScala
}
