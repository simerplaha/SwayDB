/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.map

import java.util.concurrent.ConcurrentSkipListMap

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.map.serializer.MapSerializer

import scala.reflect.ClassTag
import scala.util.{Success, Try}

private[map] class MemoryMap[K, V: ClassTag](val skipList: ConcurrentSkipListMap[K, V],
                                             flushOnOverflow: Boolean,
                                             val fileSize: Long)(implicit ordering: Ordering[K],
                                                                 serializer: MapSerializer[K, V]) extends Map[K, V] with LazyLogging {

  private var currentBytesWritten: Long = 0

  def delete: Try[Unit] =
    Try(skipList.clear())

  def add(key: K, value: V): Try[Boolean] =
    write(MapEntry.Add(key, value))

  def remove(key: K): Try[Boolean] =
    write(MapEntry.Remove[K, V](key))

  def write(entry: MapEntry[K, V]): Try[Boolean] =
    synchronized {
      if (flushOnOverflow || currentBytesWritten == 0 || ((currentBytesWritten + entry.totalByteSize) <= fileSize)) {
        entry applyTo skipList
        currentBytesWritten += entry.totalByteSize
        Success(true)
      } else
        Success(false)
    }
}
