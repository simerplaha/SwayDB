/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.core.util

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

private[swaydb] object HashedMap {

  def concurrent[K, OV, V <: OV](nullValue: OV, initialCapacity: Option[Int] = None): Concurrent[K, OV, V] =
    initialCapacity match {
      case Some(capacity) =>
        new Concurrent[K, OV, V](new ConcurrentHashMap(capacity), nullValue)

      case None =>
        new Concurrent[K, OV, V](new ConcurrentHashMap(), nullValue)
    }

  class Concurrent[K, OV, V <: OV](private val map: ConcurrentHashMap[K, V], nullValue: OV) {
    private val counter = new AtomicLong(map.size())

    def get(key: K): OV = {
      val got = map.get(key)
      if (got == null)
        nullValue
      else
        got
    }

    def put(key: K, value: V): Unit = {
      map.put(key, value)
      counter.incrementAndGet()
    }

    def contains(key: K): Boolean =
      map.containsKey(key)

    def remove(key: K): Unit =
      if (map.remove(key) != null)
        counter.decrementAndGet()

    def size() =
      counter.get()

    def isEmpty: Boolean =
      counter.get() <= 0
  }
}
