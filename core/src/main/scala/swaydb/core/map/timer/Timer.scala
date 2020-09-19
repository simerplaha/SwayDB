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

package swaydb.core.map.timer

import java.nio.file.Path

import swaydb.IO
import swaydb.core.actor.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.data.Time
import swaydb.core.io.file.{Effect, ForceSaveApplier}
import swaydb.core.map.MapEntry
import swaydb.core.map.counter.{Counter, PersistentCounter}
import swaydb.core.map.serializer.{CounterMapEntryReader, CounterMapEntryWriter, MapEntryReader, MapEntryWriter}
import swaydb.data.config.MMAP
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._

private[core] trait Timer {
  val isEmptyTimer: Boolean

  def next: Time

  def close: Unit
}

private[core] object Timer {
  val defaultKey = Slice.emptyBytes

  val folderName = "def-timer"

  trait PersistentTimer extends Timer {
    def counter: PersistentCounter
  }

  def memory(): Timer =
    new Timer {
      val memory = Counter.memory()

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

    Counter.persistent(
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

          override def counter: PersistentCounter =
            persistentCounter
        }
    }
  }
}
