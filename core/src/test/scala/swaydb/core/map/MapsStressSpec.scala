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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.map

import swaydb.core.{TestBase, TestLimitQueues, TestTimer}
import swaydb.core.data.Memory
import swaydb.core.level.zero.LevelZeroSkipListMerger
import swaydb.core.io.file.IOEffect._
import swaydb.data.accelerate.{Accelerator, Level0Meter}
import swaydb.data.config.RecoveryMode
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.core.TestData._
import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import swaydb.core.IOAssert._
import scala.concurrent.duration._
import swaydb.core.io.file.IOEffect
import swaydb.core.queue.FileLimiter

class MapsStressSpec extends TestBase {

  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  implicit def testTimer: TestTimer = TestTimer.Empty
  implicit val fileOpenLimiter: FileLimiter = TestLimitQueues.fileOpenLimiter

  import swaydb.core.map.serializer.LevelZeroMapEntryReader._
  import swaydb.core.map.serializer.LevelZeroMapEntryWriter._

  implicit val skipListMerger = LevelZeroSkipListMerger

  val keyValueCount = 100

  "Maps.persistent" should {
    "initialise and recover over 1000 maps persistent map and on reopening them should recover state all 1000 persisted maps" in {
      val keyValues = randomKeyValues(keyValueCount)

      //disable braking
      val acceleration =
        (meter: Level0Meter) => {
          Accelerator(meter.currentMapSize, None)
        }

      def testWrite(maps: Maps[Slice[Byte], Memory.SegmentResponse]) = {
        keyValues foreach {
          keyValue =>
            maps.write(time => MapEntry.Put(keyValue.key, Memory.Put(keyValue.key, keyValue.getOrFetchValue, None, time.next))).assertGet
        }
      }

      def testRead(maps: Maps[Slice[Byte], Memory.SegmentResponse]) = {
        keyValues foreach {
          keyValue =>
            val got = maps.get(keyValue.key).assertGet
            got.isInstanceOf[Memory.Put] shouldBe true
            got.key shouldBe keyValue.key
            got.getOrFetchValue shouldBe keyValue.getOrFetchValue
        }
      }

      val dir1 = IOEffect.createDirectoryIfAbsent(testDir.resolve(1.toString))
      val dir2 = IOEffect.createDirectoryIfAbsent(testDir.resolve(2.toString))

      val map1 = Maps.persistent[Slice[Byte], Memory.SegmentResponse](dir1, mmap = true, 1.byte, acceleration, RecoveryMode.ReportFailure).assertGet
      testWrite(map1)
      testRead(map1)

      val map2 = Maps.persistent[Slice[Byte], Memory.SegmentResponse](dir2, mmap = true, 1.byte, acceleration, RecoveryMode.ReportFailure).assertGet
      testWrite(map2)
      testRead(map2)

      val map3 = Maps.memory(1.byte, acceleration)
      testWrite(map3)
      testRead(map3)

      def reopen = {
        val open1 = Maps.persistent[Slice[Byte], Memory.SegmentResponse](dir1, mmap = false, 1.byte, acceleration, RecoveryMode.ReportFailure).assertGet
        testRead(open1)
        val open2 = Maps.persistent[Slice[Byte], Memory.SegmentResponse](dir2, mmap = true, 1.byte, acceleration, RecoveryMode.ReportFailure).assertGet
        testRead(open2)

        open1.close.assertGet
        open2.close.assertGet
      }

      reopen
      reopen //reopen again

      map1.close.assertGet
      map2.close.assertGet
      map2.close.assertGet

      println("total number of maps recovered: " + dir1.folders.size)
    }

  }
}
