/*
 * Copyright (c) 19/12/21, 7:29 pm Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package swaydb.core.log.timer

import swaydb.config.MMAP
import swaydb.core.file.sweeper.bytebuffer.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.file.ForceSaveApplier
import swaydb.core.log.counter.PersistentCounterLog
import swaydb.core.log.LogEntry
import swaydb.core.segment.data.Time
import swaydb.slice.Slice
import swaydb.IO
import swaydb.core.log.serialiser.{KeyValueLogEntryReader, KeyValueLogEntryWriter, LogEntryReader, LogEntryWriter}
import swaydb.effect.Effect
import swaydb.utils.StorageUnits._

import java.nio.file.Path

object PersistentTimer {

  def apply(path: Path,
            mmap: MMAP.Log,
            mod: Long = 100000,
            fileSize: Int = 1.mb)(implicit bufferCleaner: ByteBufferSweeperActor,
                                  forceSaveApplier: ForceSaveApplier): IO[swaydb.Error.Log, PersistentTimer] = {
    implicit val writer: LogEntryWriter[LogEntry.Put[Slice[Byte], Slice[Byte]]] = KeyValueLogEntryWriter.KeyValueLogEntryPutWriter
    implicit val reader: LogEntryReader[LogEntry[Slice[Byte], Slice[Byte]]] = KeyValueLogEntryReader.KeyValueLogEntryPutReader

    val timerFolder = path.resolve("def-timer")
    Effect createDirectoriesIfAbsent timerFolder

    PersistentCounterLog(
      path = timerFolder,
      mmap = mmap,
      mod = mod,
      fileSize = fileSize
    ) transform {
      persistentCounter =>
        new PersistentTimer(persistentCounter)
    }
  }
}

class PersistentTimer private(val counter: PersistentCounterLog) extends Timer {

  override val isEmptyTimer: Boolean =
    false

  override def next: Time =
    Time(counter.next)

  override def close(): Unit =
    counter.close()
}
