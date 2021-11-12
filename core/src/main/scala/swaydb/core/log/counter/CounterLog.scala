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

package swaydb.core.log.counter

import swaydb.IO
import swaydb.core.io.file.ForceSaveApplier
import swaydb.core.log.LogEntry
import swaydb.core.log.serializer.{LogEntryReader, LogEntryWriter}
import swaydb.core.sweeper.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.data.config.MMAP
import swaydb.data.slice.Slice

import java.nio.file.Path

private[swaydb] trait CounterLog {
  def next: Long

  def close: Unit
}

private[swaydb] object CounterLog {
  val startId = 10L //use 10 instead of 0 to allow format changes.

  val defaultKey: Slice[Byte] = Slice.emptyBytes

  def memory(): MemoryCounterLog =
    MemoryCounterLog()

  def persistent(dir: Path,
                 mmap: MMAP.Log,
                 mod: Long,
                 fileSize: Int)(implicit bufferCleaner: ByteBufferSweeperActor,
                                 forceSaveApplier: ForceSaveApplier,
                                 writer: LogEntryWriter[LogEntry.Put[Slice[Byte], Slice[Byte]]],
                                 reader: LogEntryReader[LogEntry[Slice[Byte], Slice[Byte]]]): IO[swaydb.Error.Log, PersistentCounterLog] =
    PersistentCounterLog(
      path = dir,
      mmap = mmap,
      mod = mod,
      fileSize = fileSize
    )
}
