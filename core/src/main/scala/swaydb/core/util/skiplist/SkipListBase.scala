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
import java.util.function.{BiConsumer, Consumer}

import swaydb.core.util.NullOps
import swaydb.data.slice.Slice

import scala.annotation.tailrec
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

private[core] trait SkipListBase[OptionKey, OptionValue, Key <: OptionKey, Value <: OptionValue, SL <: util.NavigableMap[Key, Value]] extends SkipList[OptionKey, OptionValue, Key, Value] {

  protected def skipList: SL

  override def get(key: Key): OptionValue =
    toOptionValue(skipList.get(key))

  override def remove(key: Key): Unit =
    skipList.remove(key)

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

  def headKeyOrNull: Key =
    NullOps.tryOrNull(skipList.firstKey()).asInstanceOf[Key]

  def pollLastEntry(): Map.Entry[Key, Value] =
    skipList.pollLastEntry()

  def pollFirstEntry(): Map.Entry[Key, Value] =
    skipList.pollFirstEntry()

  def headKeyValue: Option[(Key, Value)] =
    tryOptionKeyValue(skipList.firstEntry())

  def lastKeyValue: Option[(Key, Value)] =
    tryOptionKeyValue(skipList.lastEntry())

  def lastKey: OptionKey =
    tryOptionKey(skipList.lastKey())

  def lastKeyOrNull: Key =
    NullOps.tryOrNull(skipList.lastKey()).asInstanceOf[Key]

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

  def values(): Iterable[Value] =
    skipList.values().asScala

  def keys(): util.NavigableSet[Key] =
    skipList.navigableKeySet()

  def take(count: Int)(implicit classTag: ClassTag[Value]): Slice[Value] = {
    val slice = Slice.of[Value](count)

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
    val slice = Slice.of[V2](size)
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
