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
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.core.map

import java.nio.file.Path
import java.util.concurrent.ConcurrentSkipListMap

import com.typesafe.scalalogging.LazyLogging
import swaydb.IO
import swaydb.core.actor.FileSweeper
import swaydb.core.function.FunctionStore
import swaydb.core.actor.ByteBufferSweeper
import swaydb.core.actor.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.map.serializer.{MapEntryReader, MapEntryWriter}
import swaydb.core.util.IDGenerator
import swaydb.core.util.skiplist.{SkipList, SkipListBase}
import swaydb.data.config.MMAP
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._

private[core] object Map extends LazyLogging {

  /**
   * Used to assign unique numbers for each [[Map]] instance.
   */
  private[map] val uniqueFileNumberGenerator = IDGenerator()

  def persistent[OK, OV, K <: OK, V <: OV](nullKey: OK,
                                           nullValue: OV,
                                           folder: Path,
                                           mmap: MMAP.Map,
                                           flushOnOverflow: Boolean,
                                           fileSize: Long,
                                           dropCorruptedTailEntries: Boolean)(implicit keyOrder: KeyOrder[K],
                                                                              timeOrder: TimeOrder[Slice[Byte]],
                                                                              functionStore: FunctionStore,
                                                                              fileSweeper: FileSweeper,
                                                                              bufferCleaner: ByteBufferSweeperActor,
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
                                           mmap: MMAP.Map,
                                           flushOnOverflow: Boolean,
                                           fileSize: Long)(implicit keyOrder: KeyOrder[K],
                                                           timeOrder: TimeOrder[Slice[Byte]],
                                                           functionStore: FunctionStore,
                                                           fileSweeper: FileSweeper,
                                                           bufferCleaner: ByteBufferSweeperActor,
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
      _skipList = SkipList.concurrent[OK, OV, K, V](nullKey, nullValue)(keyOrder),
      flushOnOverflow = flushOnOverflow,
      fileSize = fileSize
    )
}

private[core] trait Map[OK, OV, K <: OK, V <: OV] extends SkipListBase[OK, OV, K, V, ConcurrentSkipListMap[K, V]] {

  def hasRange: Boolean

  def skipListKeyValuesMaxCount: Int

  val fileSize: Long

  def writeSync(mapEntry: MapEntry[K, V]): Boolean

  def writeNoSync(mapEntry: MapEntry[K, V]): Boolean

  def writeSafe[E: IO.ExceptionHandler](mapEntry: MapEntry[K, V]): IO[E, Boolean] =
    IO[E, Boolean](writeSync(mapEntry))

  def delete: Unit

  def exists =
    true

  def pathOption: Option[Path] =
    None

  def close(): Unit

  def uniqueFileNumber: Long
}
