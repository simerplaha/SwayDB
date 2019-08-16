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

import scala.collection.JavaConverters._
import scala.collection.mutable

object HashMap {

  def concurrent[K, V](initialCapacity: Option[Int] = None) =
    new Concurrent[K, V](
      initialCapacity map {
        capacity =>
          new ConcurrentHashMap[K, V](capacity)
      } getOrElse {
        new ConcurrentHashMap[K, V]()
      }
    )

  class Concurrent[K, V](map: ConcurrentHashMap[K, V]) {
    def get(key: K): Option[V] =
      Option(map.get(key))

    def put(key: K, value: V): Unit =
      map.put(key, value)

    def contains(key: K): Boolean =
      map.containsKey(key)

    def head =
      asScala.head

    def last =
      asScala.last

    def asScala: mutable.Map[K, V] =
      map.asScala

    def clear() =
      map.clear()
  }
}
