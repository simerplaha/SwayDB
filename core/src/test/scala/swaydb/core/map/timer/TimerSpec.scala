/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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
 */

package swaydb.core.map.timer

import java.nio.file.Path

import swaydb.core.RunThis._
import swaydb.core.TestBase
import swaydb.core.function.FunctionStore
import swaydb.core.map.MapEntry
import swaydb.core.map.serializer.{MapEntryReader, MapEntryWriter, TimerMapEntryReader, TimerMapEntryWriter}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.util.ByteUtil
import swaydb.IOValues._

import scala.concurrent.ExecutionContext

class PersistentTimerSpec extends TimerSpec {

  override def persistent: Boolean = true

  def newTimer(path: Path)(implicit keyOrder: KeyOrder[Slice[Byte]],
                           timeOrder: TimeOrder[Slice[Byte]],
                           functionStore: FunctionStore,
                           ec: ExecutionContext,
                           writer: MapEntryWriter[MapEntry.Put[Slice[Byte], Slice[Byte]]],
                           reader: MapEntryReader[MapEntry[Slice[Byte], Slice[Byte]]]): Timer =
    Timer.persistent(path, true, 100, 1000).get
}

class MemoryTimerSpec extends TimerSpec {

  override def persistent: Boolean = false

  def newTimer(path: Path)(implicit keyOrder: KeyOrder[Slice[Byte]],
                           timeOrder: TimeOrder[Slice[Byte]],
                           functionStore: FunctionStore,
                           ec: ExecutionContext,
                           writer: MapEntryWriter[MapEntry.Put[Slice[Byte], Slice[Byte]]],
                           reader: MapEntryReader[MapEntry[Slice[Byte], Slice[Byte]]]): Timer =
    Timer.memory()
}

sealed trait TimerSpec extends TestBase {

  implicit val keyOrder = KeyOrder.default
  implicit val timeOrder = TimeOrder.long
  implicit val functionStore = FunctionStore.memory()
  implicit val timerReader = TimerMapEntryReader.TimerPutMapEntryReader
  implicit val timerWriter = TimerMapEntryWriter.TimerPutMapEntryWriter

  def newTimer(path: Path)(implicit keyOrder: KeyOrder[Slice[Byte]],
                           timeOrder: TimeOrder[Slice[Byte]],
                           functionStore: FunctionStore,
                           ec: ExecutionContext,
                           writer: MapEntryWriter[MapEntry.Put[Slice[Byte], Slice[Byte]]],
                           reader: MapEntryReader[MapEntry[Slice[Byte], Slice[Byte]]]): Timer

  "it" should {

    "write time sequentially" in {

      def write(range: Range, timer: Timer) =
        range foreach {
          i =>
            val nextTime = timer.next.time
            val nextTimeLong = ByteUtil.readUnsignedLongRightAligned(nextTime).value
            nextTimeLong shouldBe i
        }

      val dir = randomDir
      val timer: Timer = newTimer(dir)
      write(1 to 1000, timer)
      timer.close.get

      if (persistent) {
        val reopenedTimer = Timer.persistent(dir, true, 100, 1000).get
        write(1000 + 101 to 2000 + 201, reopenedTimer)
        reopenedTimer.close.get

        val reopenedTimer2 = Timer.persistent(dir, true, 100, 1000).get
        write(2000 + 201 to 300 + 301, reopenedTimer2)
        reopenedTimer2.close.get
      }
    }
  }
}
