/*
 * Copyright (c) 2020 Simer Plaha (@simerplaha)
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

package swaydb.core.map

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.function.FunctionStore
import swaydb.core.util.SkipList
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice

private[map] class MemoryMap[OK, OV, K <: OK, V <: OV](val skipList: SkipList.Concurrent[OK, OV, K, V],
                                                       flushOnOverflow: Boolean,
                                                       val fileSize: Long)(implicit keyOrder: KeyOrder[K],
                                                                           timeOrder: TimeOrder[Slice[Byte]],
                                                                           functionStore: FunctionStore,
                                                                           skipListMerger: SkipListMerger[OK, OV, K, V]) extends Map[OK, OV, K, V] with LazyLogging {

  private var currentBytesWritten: Long = 0
  var skipListKeyValuesMaxCount: Int = 0

  @volatile private var _hasRange: Boolean = false

  override def hasRange: Boolean = _hasRange

  def delete: Unit =
    skipList.clear()

  override def write(entry: MapEntry[K, V]): Boolean =
    synchronized {
      if (flushOnOverflow || currentBytesWritten == 0 || ((currentBytesWritten + entry.totalByteSize) <= fileSize)) {
        if (entry.hasRange) {
          _hasRange = true //set hasRange to true before inserting so that reads start looking for floor key-values as the inserts are occurring.
          skipListMerger.insert(entry, skipList)
        } else if (entry.hasUpdate || entry.hasRemoveDeadline || _hasRange) {
          skipListMerger.insert(entry, skipList)
        } else {
          entry applyTo skipList
        }
        skipListKeyValuesMaxCount += entry.entriesCount
        currentBytesWritten += entry.totalByteSize
        true
      } else {
        false
      }
    }

  override def close(): Unit =
    ()

  def fileId: Long =
    0

}
