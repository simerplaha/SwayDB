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
