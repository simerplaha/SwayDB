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

package swaydb.core.util.series

import java.util
import java.util.Comparator

sealed trait LeafOrBranch[K, V] {
  def headKey: Option[K]

  def lastKey: Option[K]
}

class Leaf[K, V](val key: K, @volatile var value: V) extends LeafOrBranch[K, V] with util.Map.Entry[K, V] {
  override def headKey: Option[K] = Some(key)

  override def lastKey: Option[K] = Some(key)

  override def getKey: K = key

  override def getValue: V = value

  override def setValue(value: V): V = {
    val old = this.value
    this.value = value
    old
  }
}

class Branch[K, V](val slice: Series[LeafOrBranch[K, V]]) extends LeafOrBranch[K, V] {
  override def headKey: Option[K] = slice.headOption.flatMap(_.headKey)

  override def lastKey: Option[K] = slice.lastOption.flatMap(_.lastKey)
}

private[core] class SeriesNavigable[Key, Value](series: Series[LeafOrBranch[Key, Value]]) extends util.NavigableMap[Key, Value] {

  val branchLength = series.length

  def isVolatile = series.isVolatile

  override def put(key: Key, value: Value): Value = ???

  override def remove(key: Any): Value = ???

  override def putAll(m: util.Map[_ <: Key, _ <: Value]): Unit = ???

  override def lowerEntry(key: Key): util.Map.Entry[Key, Value] = ???

  override def lowerKey(key: Key): Key = ???

  override def floorEntry(key: Key): util.Map.Entry[Key, Value] = ???

  override def floorKey(key: Key): Key = ???

  override def ceilingEntry(key: Key): util.Map.Entry[Key, Value] = ???

  override def ceilingKey(key: Key): Key = ???

  override def higherEntry(key: Key): util.Map.Entry[Key, Value] = ???

  override def higherKey(key: Key): Key = ???

  override def firstEntry(): util.Map.Entry[Key, Value] = ???

  override def lastEntry(): util.Map.Entry[Key, Value] = ???

  override def pollFirstEntry(): util.Map.Entry[Key, Value] = ???

  override def pollLastEntry(): util.Map.Entry[Key, Value] = ???

  override def descendingMap(): util.NavigableMap[Key, Value] = ???

  override def navigableKeySet(): util.NavigableSet[Key] = ???

  override def descendingKeySet(): util.NavigableSet[Key] = ???

  override def subMap(fromKey: Key, fromInclusive: Boolean, toKey: Key, toInclusive: Boolean): util.NavigableMap[Key, Value] = ???

  override def headMap(toKey: Key, inclusive: Boolean): util.NavigableMap[Key, Value] = ???

  override def tailMap(fromKey: Key, inclusive: Boolean): util.NavigableMap[Key, Value] = ???

  override def subMap(fromKey: Key, toKey: Key): util.SortedMap[Key, Value] = ???

  override def headMap(toKey: Key): util.SortedMap[Key, Value] = ???

  override def tailMap(fromKey: Key): util.SortedMap[Key, Value] = ???

  override def comparator(): Comparator[_ >: Key] = ???

  override def firstKey(): Key = ???

  override def lastKey(): Key = ???

  override def keySet(): util.Set[Key] = ???

  override def values(): util.Collection[Value] = ???

  override def entrySet(): util.Set[util.Map.Entry[Key, Value]] = ???

  override def size(): Int = ???

  override def isEmpty: Boolean = ???

  override def containsKey(key: Any): Boolean = ???

  override def containsValue(value: Any): Boolean = ???

  override def get(key: Any): Value = ???

  override def clear(): Unit = ???

  override def clone(): SeriesNavigable[Key, Value] =
    ???
}