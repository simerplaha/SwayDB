/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb.core.util.skiplist

import swaydb.Bag
import swaydb.core.util.AtomicRanges
import swaydb.data.order.KeyOrder

private[core] trait SkipList[OK, OV, K <: OK, V <: OV] {

  val keyOrder: KeyOrder[K]

  private val ranges = AtomicRanges[K]()(keyOrder)

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

  def subMapValues(from: K, fromInclusive: Boolean, to: K, toInclusive: Boolean): Iterable[V]

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

  def atomicWrite[T, BAG[_]](from: K, to: K, toInclusive: Boolean)(f: => T)(implicit bag: Bag[BAG]): BAG[T] =
    AtomicRanges.writeAndRelease(from, to, toInclusive, f)(bag, ranges)

  def atomicRead[BAG[_]](getKeys: V => (K, K, Boolean))(f: SkipList[OK, OV, K, V] => OV)(implicit bag: Bag[BAG]): BAG[OV] =
    AtomicRanges.readAndRelease(getKeys, nullValue, f(this))(bag, ranges)

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
