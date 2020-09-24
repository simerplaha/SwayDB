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
  def notContains(key: Key): Boolean =
    !contains(key)

  def subMap(from: Key, fromInclusive: Boolean, to: Key, toInclusive: Boolean): Iterable[(Key, Value)]

  def headKey: OptionKey
  def lastKey: OptionKey

  def count(): Int
  def last(): OptionValue
  def head(): OptionValue
  def headKeyValue: Option[(Key, Value)]
  def values(): Iterable[Value]
  def foldLeft[R](r: R)(f: (R, (Key, Value)) => R): R
  def foreach[R](f: (Key, Value) => R): Unit
  def asScala: Iterable[(Key, Value)]

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

  sealed trait Batch[Key, +Value] {
    def apply[VV >: Value](skipList: SkipList[_, _, Key, VV]): Unit
  }
}
