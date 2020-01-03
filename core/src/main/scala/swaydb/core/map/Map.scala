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

import java.nio.file.Path

import com.typesafe.scalalogging.LazyLogging
import swaydb.IO
import swaydb.core.actor.FileSweeper
import swaydb.core.function.FunctionStore
import swaydb.core.map.serializer.{MapEntryReader, MapEntryWriter}
import swaydb.core.util.SkipList
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._

private[core] object Map extends LazyLogging {

  def persistent[OK, OV, K <: OK, V <: OV](nullKey: OK,
                                           nullValue: OV,
                                           folder: Path,
                                           mmap: Boolean,
                                           flushOnOverflow: Boolean,
                                           fileSize: Long,
                                           dropCorruptedTailEntries: Boolean)(implicit keyOrder: KeyOrder[K],
                                                                              timeOrder: TimeOrder[Slice[Byte]],
                                                                              functionStore: FunctionStore,
                                                                              fileSweeper: FileSweeper,
                                                                              writer: MapEntryWriter[MapEntry.Put[K, V]],
                                                                              reader: MapEntryReader[MapEntry[K, V]],
                                                                              skipListMerge: SkipListMerger[OK, OV, K, V]): RecoveryResult[PersistentMap[OK, OV, K, V]] =
    PersistentMap(
      folder = folder,
      mmap = mmap,
      flushOnOverflow = flushOnOverflow,
      fileSize = fileSize,
      dropCorruptedTailEntries = dropCorruptedTailEntries,
      nullKey = nullKey,
      nullValue = nullValue
    )

  def persistent[OK, OV, K <: OK, V <: OV](nullKey: OK,
                                           nullValue: OV,
                                           folder: Path,
                                           mmap: Boolean,
                                           flushOnOverflow: Boolean,
                                           fileSize: Long)(implicit keyOrder: KeyOrder[K],
                                                           timeOrder: TimeOrder[Slice[Byte]],
                                                           functionStore: FunctionStore,
                                                           fileSweeper: FileSweeper,
                                                           writer: MapEntryWriter[MapEntry.Put[K, V]],
                                                           skipListMerger: SkipListMerger[OK, OV, K, V]): PersistentMap[OK, OV, K, V] =
    PersistentMap(
      folder = folder,
      mmap = mmap,
      flushOnOverflow = flushOnOverflow,
      fileSize = fileSize,
      nullKey = nullKey,
      nullValue = nullValue
    )

  def memory[OK, OV, K <: OK, V <: OV](nullKey: OK,
                                       nullValue: OV,
                                       fileSize: Long = 0.byte,
                                       flushOnOverflow: Boolean = true)(implicit keyOrder: KeyOrder[K],
                                                                        timeOrder: TimeOrder[Slice[Byte]],
                                                                        functionStore: FunctionStore,
                                                                        skipListMerge: SkipListMerger[OK, OV, K, V]): MemoryMap[OK, OV, K, V] =
    new MemoryMap[OK, OV, K, V](
      skipList = SkipList.concurrent[OK, OV, K, V](nullKey, nullValue)(keyOrder),
      flushOnOverflow = flushOnOverflow,
      fileSize = fileSize
    )
}

private[core] trait Map[OK, OV, K <: OK, V <: OV] {

  def hasRange: Boolean

  val skipList: SkipList.Concurrent[OK, OV, K, V]

  def skipListKeyValuesMaxCount: Int

  val fileSize: Long

  def write(mapEntry: MapEntry[K, V]): Boolean

  def writeSafe[E: IO.ExceptionHandler](mapEntry: MapEntry[K, V]): IO[E, Boolean] =
    IO[E, Boolean](write(mapEntry))

  def delete: Unit

  def size: Int =
    skipList.size

  def isEmpty: Boolean =
    skipList.isEmpty

  def exists =
    true

  def pathOption: Option[Path] =
    None

  def close(): Unit

  def fileId: Long
}
