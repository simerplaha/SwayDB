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

package swaydb.core.map.timer

import swaydb.IO
import swaydb.core.data.Time
import swaydb.core.io.file.ForceSaveApplier
import swaydb.core.map.MapEntry
import swaydb.core.map.counter.{CounterMap, PersistentCounterMap}
import swaydb.core.map.serializer.{CounterMapEntryReader, CounterMapEntryWriter, MapEntryReader, MapEntryWriter}
import swaydb.core.sweeper.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.data.config.MMAP
import swaydb.data.slice.Slice
import swaydb.effect.Effect
import swaydb.utils.StorageUnits._

import java.nio.file.Path

private[core] trait Timer {
  val isEmptyTimer: Boolean

  def next: Time

  def close: Unit
}

private[core] object Timer {
  val defaultKey = Slice.emptyBytes

  val folderName = "def-timer"

  trait PersistentTimer extends Timer {
    def counter: PersistentCounterMap
  }

  def memory(): Timer =
    new Timer {
      val memory = CounterMap.memory()

      override val isEmptyTimer: Boolean =
        false

      override def next: Time =
        Time(memory.next)

      override def close: Unit =
        memory.close
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
                 mmap: MMAP.Map,
                 mod: Long = 100000,
                 fileSize: Long = 1.mb)(implicit bufferCleaner: ByteBufferSweeperActor,
                                        forceSaveApplier: ForceSaveApplier): IO[swaydb.Error.Map, PersistentTimer] = {
    implicit val writer: MapEntryWriter[MapEntry.Put[Slice[Byte], Slice[Byte]]] = CounterMapEntryWriter.CounterPutMapEntryWriter
    implicit val reader: MapEntryReader[MapEntry[Slice[Byte], Slice[Byte]]] = CounterMapEntryReader.CounterPutMapEntryReader

    val timerFolder = path.resolve(folderName)
    Effect createDirectoriesIfAbsent timerFolder

    CounterMap.persistent(
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

          override def close: Unit =
            persistentCounter.close

          override def counter: PersistentCounterMap =
            persistentCounter
        }
    }
  }
}
