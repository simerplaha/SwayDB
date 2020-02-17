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
import java.util.concurrent.ConcurrentSkipListMap

import swaydb.Bagged
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice

import scala.collection.mutable

private[core] trait SkipList[OptionKey, OptionValue, Key <: OptionKey, Value <: OptionValue] {
  def nullKey: OptionKey
  def nullValue: OptionValue

  def put(key: Key, value: Value): Unit
  def putIfAbsent(key: Key, value: Value): Boolean
  def get(key: Key): OptionValue
  def remove(key: Key): Unit

  def lower(key: Key): OptionValue
  def lowerKey(key: Key): OptionKey

  def floor(key: Key): OptionValue
  def floorKeyValue(key: Key): Option[(Key, Value)]

  def higher(key: Key): OptionValue
  def higherKey(key: Key): OptionKey
  def higherKeyValue(key: Key): Option[(Key, Value)]

  def ceiling(key: Key): OptionValue
  def ceilingKey(key: Key): OptionKey

  def isEmpty: Boolean
  def nonEmpty: Boolean
  def clear(): Unit
  def size: Int
  def contains(key: Key): Boolean
  def headKey: OptionKey
  def lastKey: OptionKey

  def count(): Int
  def last(): OptionValue
  def head(): OptionValue
  def headKeyValue: Option[(Key, Value)]
  def values(): util.Collection[Value]
  def keys(): util.NavigableSet[Key]
  def take(count: Int): Slice[Value]
  def foldLeft[R](r: R)(f: (R, (Key, Value)) => R): R
  def foreach[R](f: (Key, Value) => R): Unit
  def subMap(from: Key, to: Key): util.NavigableMap[Key, Value]
  def subMap(from: Key, fromInclusive: Boolean, to: Key, toInclusive: Boolean): util.NavigableMap[Key, Value]
  def asScala: mutable.Map[Key, Value]
  def isConcurrent: Boolean

  @inline final def toOptionValue(entry: java.util.Map.Entry[Key, Value]): OptionValue =
    if (entry == null)
      nullValue
    else
      entry.getValue

  @inline final def toOptionValue(value: Value): OptionValue =
    if (value == null)
      nullValue
    else
      value

  @inline final def toOptionKey(key: Key): OptionKey =
    if (key == null)
      nullKey
    else
      key

  @inline final def toOptionKeyValue(entry: java.util.Map.Entry[Key, Value]): Option[(Key, Value)] =
    if (entry == null)
      None
    else
      Option((entry.getKey, entry.getValue))

  @inline final def tryOptionValue(block: => java.util.Map.Entry[Key, Value]): OptionValue =
    try
      toOptionValue(block)
    catch {
      case _: Throwable =>
        nullValue
    }

  @inline final def tryOptionKey(block: => Key): OptionKey =
    try
      toOptionKey(block)
    catch {
      case _: Throwable =>
        nullKey
    }

  @inline final def tryOptionKeyValue(block: => java.util.Map.Entry[Key, Value]): Option[(Key, Value)] =
    try
      toOptionKeyValue(block)
    catch {
      case _: Throwable =>
        None
    }
}

private[core] object SkipList {
  object Batch {
    case class Remove[Key](key: Key) extends Batch[Key, Nothing] {
      override def apply[VV >: Nothing](skipList: SkipList[_, _, Key, VV]): Unit =
        skipList.remove(key)
    }
    case class Put[Key, Value](key: Key, value: Value) extends Batch[Key, Value] {
      override def apply[VV >: Value](skipList: SkipList[_, _, Key, VV]): Unit =
        skipList.put(key, value)
    }
  }

  sealed trait KeyValue[Key, Value] extends Bagged[(Key, Value), Option]
  object KeyValue {
    case class Some[Key, Value](key: Key, value: Value) extends KeyValue[Key, Value] {
      def tuple: (Key, Value) =
        (key, value)

      override def get: Option[(Key, Value)] =
        Option(tuple)
    }

    case object None extends KeyValue[Nothing, Nothing] {
      override def get = Option.empty
    }
  }

  sealed trait Batch[Key, +Value] {
    def apply[VV >: Value](skipList: SkipList[_, _, Key, VV]): Unit
  }

  def concurrent[OptionKey, OptionValue, Key <: OptionKey, Value <: OptionValue](nullKey: OptionKey,
                                                                                 nullValue: OptionValue)(implicit ordering: KeyOrder[Key]): SkipListConcurrent[OptionKey, OptionValue, Key, Value] =
    new SkipListConcurrent[OptionKey, OptionValue, Key, Value](
      skipper = new ConcurrentSkipListMap[Key, Value](ordering),
      nullKey = nullKey,
      nullValue = nullValue
    )

  def map[OptionKey, OptionValue, Key <: OptionKey, Value <: OptionValue](nullKey: OptionKey,
                                                                          nullValue: OptionValue)(implicit ordering: KeyOrder[Key]): SkipListMap[OptionKey, OptionValue, Key, Value] =
    new SkipListMap[OptionKey, OptionValue, Key, Value](
      skipper = new util.TreeMap[Key, Value](ordering),
      nullKey = nullKey,
      nullValue = nullValue
    )

  def concurrent[OptionKey, OptionValue, Key <: OptionKey, Value <: OptionValue](limit: Int,
                                                                                 nullKey: OptionKey,
                                                                                 nullValue: OptionValue)(implicit ordering: KeyOrder[Key]): SkipListConcurrentLimit[OptionKey, OptionValue, Key, Value] =
    new SkipListConcurrentLimit[OptionKey, OptionValue, Key, Value](
      limit = limit,
      skipList =
        SkipList.concurrent[OptionKey, OptionValue, Key, Value](
          nullKey = nullKey,
          nullValue = nullValue
        ),
      nullKey = nullKey,
      nullValue = nullValue
    )

}
