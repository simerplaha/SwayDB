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

import swaydb.Bag
import swaydb.core.util.AtomicRanges
import swaydb.data.order.KeyOrder

private[core] trait SkipList[OK, OV, K <: OK, V <: OV] {

  def keyOrder: KeyOrder[K]

  private lazy val ranges = AtomicRanges[K]()(keyOrder)

  def nullKey: OK
  def nullValue: OV

  def put(key: K, value: V): Unit
  def putIfAbsent(key: K, value: V): Boolean
  def get(key: K): OV
  def remove(key: K): Unit

  def lower(key: K): OV
  def lowerKey(key: K): OK

  def floor(key: K): OV
  def floorKeyValue(key: K): Option[(K, V)]

  def higher(key: K): OV
  def higherKey(key: K): OK
  def higherKeyValue(key: K): Option[(K, V)]

  def ceiling(key: K): OV
  def ceilingKey(key: K): OK

  def isEmpty: Boolean
  def nonEmpty: Boolean
  def clear(): Unit
  def size: Int
  def contains(key: K): Boolean
  def notContains(key: K): Boolean =
    !contains(key)

  def subMap(from: K, fromInclusive: Boolean, to: K, toInclusive: Boolean): Iterable[(K, V)]

  def headKey: OK
  def lastKey: OK

  def count(): Int
  def last(): OV
  def head(): OV
  def headKeyValue: Option[(K, V)]
  def values(): Iterable[V]
  def foldLeft[R](r: R)(f: (R, (K, V)) => R): R
  def foreach[R](f: (K, V) => R): Unit
  def toIterable: Iterable[(K, V)]
  def iterator: Iterator[(K, V)]
  def valuesIterator: Iterator[V]

  def transaction[T, BAG[_]](from: K, to: K, toInclusive: Boolean, action: AtomicRanges.Action)(f: => T)(implicit bag: Bag[BAG]): BAG[T] =
    ranges.execute(from, to, toInclusive, action)(f)

  @inline final def toOptionValue(entry: java.util.Map.Entry[K, V]): OV =
    if (entry == null)
      nullValue
    else
      entry.getValue

  @inline final def toOptionValue(value: V): OV =
    if (value == null)
      nullValue
    else
      value

  @inline final def toOptionKey(key: K): OK =
    if (key == null)
      nullKey
    else
      key

  @inline final def toOptionKeyValue(entry: java.util.Map.Entry[K, V]): Option[(K, V)] =
    if (entry == null)
      None
    else
      Option((entry.getKey, entry.getValue))

  @inline final def tryOptionValue(block: => java.util.Map.Entry[K, V]): OV =
    try
      toOptionValue(block)
    catch {
      case _: Throwable =>
        nullValue
    }

  @inline final def tryOptionKey(block: => K): OK =
    try
      toOptionKey(block)
    catch {
      case _: Throwable =>
        nullKey
    }

  @inline final def tryOptionKeyValue(block: => java.util.Map.Entry[K, V]): Option[(K, V)] =
    try
      toOptionKeyValue(block)
    catch {
      case _: Throwable =>
        None
    }
}
