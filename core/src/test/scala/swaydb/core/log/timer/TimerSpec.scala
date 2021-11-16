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

import swaydb.core.function.FunctionStore
import swaydb.core.io.file.ForceSaveApplier
import swaydb.core.log.LogEntry
import swaydb.core.log.MapTestUtil._
import swaydb.core.log.counter.CounterLog
import swaydb.core.log.serializer.{CounterLogEntryReader, CounterLogEntryWriter, LogEntryReader, LogEntryWriter}
import swaydb.core.sweeper.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.{TestBase, TestCaseSweeper, TestExecutionContext, TestForceSave}
import swaydb.data.config.MMAP
import swaydb.slice.order.{KeyOrder, TimeOrder}
import swaydb.slice.Slice
import swaydb.utils.OperatingSystem

import java.nio.file.Path
import scala.concurrent.ExecutionContext

class PersistentTimerSpec extends TimerSpec {

  override def persistent: Boolean = true

  def newTimer(path: Path)(implicit ec: ExecutionContext,
                           forceSaveApplier: ForceSaveApplier,
                           cleaner: ByteBufferSweeperActor,
                           writer: LogEntryWriter[LogEntry.Put[Slice[Byte], Slice[Byte]]],
                           reader: LogEntryReader[LogEntry[Slice[Byte], Slice[Byte]]]): Timer =
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
                           writer: LogEntryWriter[LogEntry.Put[Slice[Byte], Slice[Byte]]],
                           reader: LogEntryReader[LogEntry[Slice[Byte], Slice[Byte]]]): Timer =
    Timer.memory()
}

sealed trait TimerSpec extends TestBase {

  implicit val ec = TestExecutionContext.executionContext
  implicit val keyOrder = KeyOrder.default
  implicit val timeOrder = TimeOrder.long
  implicit val functionStore = FunctionStore.memory()
  implicit val timerReader = CounterLogEntryReader.CounterPutLogEntryReader
  implicit val timerWriter = CounterLogEntryWriter.CounterPutLogEntryWriter

  def newTimer(path: Path)(implicit ec: ExecutionContext,
                           forceSaveApplier: ForceSaveApplier,
                           cleaner: ByteBufferSweeperActor,
                           writer: LogEntryWriter[LogEntry.Put[Slice[Byte], Slice[Byte]]],
                           reader: LogEntryReader[LogEntry[Slice[Byte], Slice[Byte]]]): Timer

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
          write((CounterLog.startId.toInt + 1) to 1000, timer)
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
