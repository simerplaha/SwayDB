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

package swaydb.core.map.counter

import swaydb.IO
import swaydb.core.io.file.ForceSaveApplier
import swaydb.core.map.MapEntry
import swaydb.core.map.serializer.{MapEntryReader, MapEntryWriter}
import swaydb.core.sweeper.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.data.config.MMAP
import swaydb.data.slice.Slice

import java.nio.file.Path

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
