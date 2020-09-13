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
 * If you modify this Program, or any covered work, by linking or combining
 * it with other code, such other code is not for that reason alone subject
 * to any of the requirements of the GNU Affero GPL version 3.
 */

package swaydb.core.map.counter

import java.nio.file.Path

import swaydb.IO
import swaydb.core.actor.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.io.file.ForceSaveApplier
import swaydb.core.map.MapEntry
import swaydb.core.map.serializer.{MapEntryReader, MapEntryWriter}
import swaydb.data.config.MMAP
import swaydb.data.slice.Slice

private[swaydb] trait Counter {
  def next: Long

  def close: Unit
}

private[swaydb] object Counter {
  val startId = 10L //use 10 instead of 0 to allow format changes.

  val defaultKey = Slice.emptyBytes

  def memory(): MemoryCounter =
    MemoryCounter()

  def persistent(path: Path,
                 mmap: MMAP.Map,
                 mod: Long,
                 flushCheckpointSize: Long)(implicit bufferCleaner: ByteBufferSweeperActor,
                                            forceSaveApplier: ForceSaveApplier,
                                            writer: MapEntryWriter[MapEntry.Put[Slice[Byte], Slice[Byte]]],
                                            reader: MapEntryReader[MapEntry[Slice[Byte], Slice[Byte]]]): IO[swaydb.Error.Map, Counter] =
    PersistentCounter(
      path = path,
      mmap = mmap,
      mod = mod,
      flushCheckpointSize = flushCheckpointSize
    )
}
