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

package swaydb.core.map.counter

import java.nio.file.Path
import swaydb.IO
import swaydb.core.io.file.ForceSaveApplier
import swaydb.core.map.MapEntry
import swaydb.core.map.serializer.{MapEntryReader, MapEntryWriter}
import swaydb.core.sweeper.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.data.config.MMAP
import swaydb.data.slice.Slice

private[swaydb] trait CounterMap {
  def next: Long

  def close: Unit
}

private[swaydb] object CounterMap {
  val startId = 10L //use 10 instead of 0 to allow format changes.

  val defaultKey: Slice[Byte] = Slice.emptyBytes

  def memory(): MemoryCounterMap =
    MemoryCounterMap()

  def persistent(dir: Path,
                 mmap: MMAP.Map,
                 mod: Long,
                 fileSize: Long)(implicit bufferCleaner: ByteBufferSweeperActor,
                                 forceSaveApplier: ForceSaveApplier,
                                 writer: MapEntryWriter[MapEntry.Put[Slice[Byte], Slice[Byte]]],
                                 reader: MapEntryReader[MapEntry[Slice[Byte], Slice[Byte]]]): IO[swaydb.Error.Map, PersistentCounterMap] =
    PersistentCounterMap(
      path = dir,
      mmap = mmap,
      mod = mod,
      fileSize = fileSize
    )
}
