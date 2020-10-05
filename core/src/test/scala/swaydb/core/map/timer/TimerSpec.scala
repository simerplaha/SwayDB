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

import swaydb.core.actor.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.function.FunctionStore
import swaydb.core.io.file.ForceSaveApplier
import swaydb.core.map.MapEntry
import swaydb.core.map.MapTestUtil._
import swaydb.core.map.counter.CounterMap
import swaydb.core.map.serializer.{CounterMapEntryReader, CounterMapEntryWriter, MapEntryReader, MapEntryWriter}
import swaydb.core.{TestBase, TestCaseSweeper, TestExecutionContext, TestForceSave}
import swaydb.data.config.MMAP
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.util.OperatingSystem

import scala.concurrent.ExecutionContext

class PersistentTimerSpec extends TimerSpec {

  override def persistent: Boolean = true

  def newTimer(path: Path)(implicit ec: ExecutionContext,
                           forceSaveApplier: ForceSaveApplier,
                           cleaner: ByteBufferSweeperActor,
                           writer: MapEntryWriter[MapEntry.Put[Slice[Byte], Slice[Byte]]],
                           reader: MapEntryReader[MapEntry[Slice[Byte], Slice[Byte]]]): Timer =
    Timer.persistent(
      path = path,
      mmap = MMAP.On(OperatingSystem.isWindows, TestForceSave.mmap()),
      mod = 100,
      fileSize = 1000
    ).get
}

class MemoryTimerSpec extends TimerSpec {

  override def persistent: Boolean = false

  def newTimer(path: Path)(implicit ec: ExecutionContext,
                           forceSaveApplier: ForceSaveApplier,
                           cleaner: ByteBufferSweeperActor,
                           writer: MapEntryWriter[MapEntry.Put[Slice[Byte], Slice[Byte]]],
                           reader: MapEntryReader[MapEntry[Slice[Byte], Slice[Byte]]]): Timer =
    Timer.memory()
}

sealed trait TimerSpec extends TestBase {

  implicit val ec = TestExecutionContext.executionContext
  implicit val keyOrder = KeyOrder.default
  implicit val timeOrder = TimeOrder.long
  implicit val functionStore = FunctionStore.memory()
  implicit val timerReader = CounterMapEntryReader.CounterPutMapEntryReader
  implicit val timerWriter = CounterMapEntryWriter.CounterPutMapEntryWriter

  def newTimer(path: Path)(implicit ec: ExecutionContext,
                           forceSaveApplier: ForceSaveApplier,
                           cleaner: ByteBufferSweeperActor,
                           writer: MapEntryWriter[MapEntry.Put[Slice[Byte], Slice[Byte]]],
                           reader: MapEntryReader[MapEntry[Slice[Byte], Slice[Byte]]]): Timer

  "it" should {

    "write time sequentially" in {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._

          def write(range: Range, timer: Timer) =
            range foreach {
              i =>
                val nextTime = timer.next.time
                nextTime.readUnsignedLong() shouldBe i
            }

          val dir = randomDir
          val timer: Timer = newTimer(dir)
          write((CounterMap.startId.toInt + 1) to 1000, timer)
          timer.close

          timer match {
            case timer: Timer.PersistentTimer =>
              val reopenedTimer = timer.reopen

              write(1000 + 101 to 2000 + 201, reopenedTimer)

              val reopenedTimer2 = reopenedTimer.reopen

              write(2000 + 201 to 300 + 301, reopenedTimer2)
              reopenedTimer2.close

            case _ =>
              //cannot reopen non-persistent timers.
          }

      }
    }
  }
}
