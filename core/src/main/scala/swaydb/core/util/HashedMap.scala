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

import java.util.concurrent.ConcurrentHashMap

import scala.jdk.CollectionConverters._
import scala.collection.mutable

private[swaydb] object HashedMap {

  def concurrent[K, V, N >: V](nullValue: N, initialCapacity: Option[Int] = None): Concurrent[K, V, N] =
    initialCapacity match {
      case Some(capacity) =>
        new Concurrent[K, V, N](new ConcurrentHashMap(capacity), nullValue)

      case None =>
        new Concurrent[K, V, N](new ConcurrentHashMap(), nullValue)
    }

  class Concurrent[K, V, N >: V](map: ConcurrentHashMap[K, V], nullValue: N) {
    def get(key: K): N = {
      val got = map.get(key)
      if (got == null)
        nullValue
      else
        got
    }

    def put(key: K, value: V): Unit =
      map.put(key, value)

    def contains(key: K): Boolean =
      map.containsKey(key)

    def remove(key: K): Unit =
      map.remove(key)

    def head: (K, V) =
      asScala.head

    def last: (K, V) =
      asScala.last

    def asScala: mutable.Map[K, V] =
      map.asScala

    def clear(): Unit =
      map.clear()
  }
}
