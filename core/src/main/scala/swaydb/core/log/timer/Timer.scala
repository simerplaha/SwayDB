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

package swaydb.core.log.timer

import swaydb.IO
import swaydb.config.MMAP
import swaydb.core.file.ForceSaveApplier
import swaydb.core.file.sweeper.bytebuffer.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.log.LogEntry
import swaydb.core.log.counter.{CounterLog, PersistentCounterLog}
import swaydb.core.log.serialiser.{CounterLogEntryReader, CounterLogEntryWriter, LogEntryReader, LogEntryWriter}
import swaydb.core.segment.data.Time
import swaydb.effect.Effect
import swaydb.slice.Slice
import swaydb.utils.StorageUnits._

import java.nio.file.Path

private[core] trait Timer {
  val isEmptyTimer: Boolean

  def next: Time

  def close(): Unit
}

private[core] object Timer {
  val defaultKey = Slice.emptyBytes

  val folderName = "def-timer"

  trait PersistentTimer extends Timer {
    def counter: PersistentCounterLog
  }

  def memory(): Timer =
    new Timer {
      val memory = CounterLog.memory()

      override val isEmptyTimer: Boolean =
        false

      override def next: Time =
        Time(memory.next)

      override def close(): Unit =
        memory.close()
    }

  def empty: Timer =
    new Timer {
      override val isEmptyTimer: Boolean =
        true

      override val next: Time =
        Time.empty

      override val close: Unit =
        ()
    }

  def persistent(path: Path,
                 mmap: MMAP.Log,
                 mod: Long = 100000,
                 fileSize: Int = 1.mb)(implicit bufferCleaner: ByteBufferSweeperActor,
                                       forceSaveApplier: ForceSaveApplier): IO[swaydb.Error.Log, PersistentTimer] = {
    implicit val writer: LogEntryWriter[LogEntry.Put[Slice[Byte], Slice[Byte]]] = CounterLogEntryWriter.CounterPutLogEntryWriter
    implicit val reader: LogEntryReader[LogEntry[Slice[Byte], Slice[Byte]]] = CounterLogEntryReader.CounterPutLogEntryReader

    val timerFolder = path.resolve(folderName)
    Effect createDirectoriesIfAbsent timerFolder

    CounterLog.persistent(
      dir = timerFolder,
      mmap = mmap,
      mod = mod,
      fileSize = fileSize
    ) transform {
      persistentCounter =>
        new PersistentTimer {
          override val isEmptyTimer: Boolean =
            false

          override def next: Time =
            Time(persistentCounter.next)

          override def close(): Unit =
            persistentCounter.close()

          override def counter: PersistentCounterLog =
            persistentCounter
        }
    }
  }
}
