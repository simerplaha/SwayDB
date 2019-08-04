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

package swaydb.core.map

import java.nio.file.Path

import com.typesafe.scalalogging.LazyLogging
import swaydb.IO
import swaydb.core.function.FunctionStore
import swaydb.core.map.serializer.{MapEntryReader, MapEntryWriter}
import swaydb.core.queue.FileLimiter
import swaydb.core.util.{ConcurrentSkipList, SkipList}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._

import scala.reflect.ClassTag

private[core] object Map extends LazyLogging {

  def persistent[K, V: ClassTag](folder: Path,
                                 mmap: Boolean,
                                 flushOnOverflow: Boolean,
                                 fileSize: Long,
                                 initialWriteCount: Long,
                                 dropCorruptedTailEntries: Boolean)(implicit keyOrder: KeyOrder[K],
                                                                    timeOrder: TimeOrder[Slice[Byte]],
                                                                    functionStore: FunctionStore,
                                                                    limiter: FileLimiter,
                                                                    writer: MapEntryWriter[MapEntry.Put[K, V]],
                                                                    reader: MapEntryReader[MapEntry[K, V]],
                                                                    skipListMerge: SkipListMerger[K, V]): IO[swaydb.Error.Map, RecoveryResult[PersistentMap[K, V]]] =
    PersistentMap(
      folder = folder,
      mmap = mmap,
      flushOnOverflow = flushOnOverflow,
      fileSize = fileSize,
      initialWriteCount = initialWriteCount,
      dropCorruptedTailEntries = dropCorruptedTailEntries
    )

  def persistent[K, V: ClassTag](folder: Path,
                                 mmap: Boolean,
                                 flushOnOverflow: Boolean,
                                 initialWriteCount: Long,
                                 fileSize: Long)(implicit keyOrder: KeyOrder[K],
                                                 timeOrder: TimeOrder[Slice[Byte]],
                                                 functionStore: FunctionStore,
                                                 limiter: FileLimiter,
                                                 reader: MapEntryReader[MapEntry[K, V]],
                                                 writer: MapEntryWriter[MapEntry.Put[K, V]],
                                                 skipListMerger: SkipListMerger[K, V]): IO[swaydb.Error.Map, PersistentMap[K, V]] =
    PersistentMap(
      folder = folder,
      mmap = mmap,
      flushOnOverflow = flushOnOverflow,
      initialWriteCount = initialWriteCount,
      fileSize = fileSize
    )

  def memory[K, V: ClassTag](fileSize: Long = 0.byte,
                             flushOnOverflow: Boolean = true)(implicit keyOrder: KeyOrder[K],
                                                              timeOrder: TimeOrder[Slice[Byte]],
                                                              functionStore: FunctionStore,
                                                              skipListMerge: SkipListMerger[K, V],
                                                              writer: MapEntryWriter[MapEntry.Put[K, V]]): MemoryMap[K, V] =
    new MemoryMap[K, V](
      skipList = SkipList.concurrent[K, V]()(keyOrder),
      flushOnOverflow = flushOnOverflow,
      fileSize = fileSize
    )
}

private[core] trait Map[K, V] {

  def hasRange: Boolean

  val skipList: ConcurrentSkipList[K, V]

  val fileSize: Long

  def writeCountStateId: Long

  def incrementWriteCountStateId: Long

  def write(mapEntry: MapEntry[K, V]): IO[swaydb.Error.Map, Boolean]

  def delete: IO[swaydb.Error.Map, Unit]

  def size: Int =
    skipList.size

  def isEmpty: Boolean =
    skipList.isEmpty

  def exists =
    true

  def pathOption: Option[Path] =
    None

  def close(): IO[swaydb.Error.Map, Unit]

  def fileId: IO[swaydb.Error.Map, Long] =
    IO.Success[swaydb.Error.Map, Long](0)
}
