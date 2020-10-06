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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.core.map

import java.nio.file.Path

import com.typesafe.scalalogging.LazyLogging
import swaydb.data.config.{ForceSave, MMAP}

protected class MemoryMap[K, V, C <: MapCache[K, V]](val cache: C,
                                                     flushOnOverflow: Boolean,
                                                     val fileSize: Long) extends Map[K, V, C] with LazyLogging {

  private var currentBytesWritten: Long = 0
  var skipListKeyValuesMaxCount: Int = 0

  override val uniqueFileNumber: Long =
    Map.uniqueFileNumberGenerator.nextID

  def delete: Unit = ()

  override def writeSync(entry: MapEntry[K, V]): Boolean =
    synchronized(writeNoSync(entry))

  override def writeNoSync(entry: MapEntry[K, V]): Boolean = {
    val entryTotalByteSize = entry.totalByteSize
    if (flushOnOverflow || currentBytesWritten == 0 || ((currentBytesWritten + entryTotalByteSize) <= fileSize)) {
      cache.writeAtomic(entry)
      skipListKeyValuesMaxCount += entry.entriesCount
      currentBytesWritten += entryTotalByteSize
      true
    } else {
      false
    }
  }

  override def mmap: MMAP.Map =
    MMAP.Off(ForceSave.Off)

  override def close(): Unit =
    ()

  override def exists: Boolean =
    true

  override def pathOption: Option[Path] =
    None
}
