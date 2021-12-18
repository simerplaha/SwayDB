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

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import swaydb.config.MMAP
import swaydb.core.{CoreSpecType, CoreTestSweeper, TestForceSave}
import swaydb.core.file.CoreFileTestKit._
import swaydb.core.log.counter.CounterLog
import swaydb.core.log.serialiser.{CounterLogEntryReader, CounterLogEntryWriter}
import swaydb.core.log.LogTestKit.ReopenTimer
import swaydb.utils.OperatingSystem

class TimerSpec extends AnyFlatSpec {

  implicit val timerReader = CounterLogEntryReader.CounterPutLogEntryReader
  implicit val timerWriter = CounterLogEntryWriter.CounterPutLogEntryWriter

  it should "write time sequentially" in {
    CoreTestSweeper.foreachParallel(CoreSpecType.all) {
      (_sweeper, _specType) =>
        implicit val sweeper: CoreTestSweeper = _sweeper
        implicit val specType: CoreSpecType = _specType

        import sweeper._

        def write(range: Range, timer: Timer) =
          range foreach {
            i =>
              val nextTime = timer.next.time
              nextTime.readUnsignedLong() shouldBe i
          }

        val dir = randomDir()

        val timer: Timer =
          specType match {
            case CoreSpecType.Memory =>
              Timer.memory()

            case CoreSpecType.Persistent =>
              Timer.persistent(
                path = dir,
                mmap = MMAP.On(OperatingSystem.isWindows(), TestForceSave.mmap()),
                mod = 100,
                fileSize = 1000
              ).get
          }

        write((CounterLog.startId.toInt + 1) to 1000, timer)
        timer.close()

        timer match {
          case timer: Timer.PersistentTimer =>
            val reopenedTimer = timer.reopen

            write(1000 + 101 to 2000 + 201, reopenedTimer)

            val reopenedTimer2 = reopenedTimer.reopen

            write(2000 + 201 to 300 + 301, reopenedTimer2)
            reopenedTimer2.close()

          case _ =>
          //cannot reopen non-persistent timers.
        }

    }
  }
}
