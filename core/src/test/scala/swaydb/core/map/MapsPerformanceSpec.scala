/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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

import swaydb.core.{TestBase, TestTimeGenerator}
import swaydb.core.data.{Memory, Value}
import swaydb.core.io.file.IO
import swaydb.core.level.zero.LevelZeroSkipListMerger
import swaydb.core.util.Benchmark
import swaydb.data.accelerate.Accelerator
import swaydb.data.config.RecoveryMode
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.data.order.{KeyOrder, TimeOrder}
import scala.concurrent.duration._
import swaydb.core.TestData._
import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import swaydb.core.TryAssert._

class MapsPerformanceSpec extends TestBase with Benchmark {

  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default
  implicit def timeGenerator: TestTimeGenerator = TestTimeGenerator.random
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long

  import swaydb.core.map.serializer.LevelZeroMapEntryReader._
  import swaydb.core.map.serializer.LevelZeroMapEntryWriter._
  implicit val skipListMerger = LevelZeroSkipListMerger

  "Maps" should {
    "write key values" in {
      //      val keyValues = randomIntKeyValues(2000000)
      val keyValues = randomKeyValues(2000)

      def testWrite(maps: Maps[Slice[Byte], Memory.SegmentResponse]) =
        keyValues foreach {
          keyValue =>
            maps.write {
              MapEntry.Put[Slice[Byte], Memory.Put](keyValue.key, Memory.put(keyValue.key, keyValue.getOrFetchValue))(Level0PutWriter)
            }.assertGet
        }

      def testRead(maps: Maps[Slice[Byte], Memory.SegmentResponse]) =
        keyValues foreach {
          keyValue =>
            maps.get(keyValue.key)
          //            maps.get(keyValue.key).assertGet shouldBe ((ValueType.Add, keyValue.getOrFetchValue.assertGetOpt))
        }

      val dir1 = IO.createDirectoryIfAbsent(testDir.resolve(1.toString))

      val map1 = Maps.persistent[Slice[Byte], Memory.SegmentResponse](dir1, mmap = true, 4.mb, Accelerator.noBrakes(), RecoveryMode.ReportFailure).assertGet
      benchmark(s"MMAP = true - writing ${keyValues.size} keys") {
        testWrite(map1)
      }
      benchmark(s"MMAP = true - reading ${keyValues.size} keys") {
        testRead(map1)
      }

      val dir2 = IO.createDirectoryIfAbsent(testDir.resolve(2.toString))
      val map2 = Maps.persistent[Slice[Byte], Memory.SegmentResponse](dir2, mmap = false, 4.mb, Accelerator.noBrakes(), RecoveryMode.ReportFailure).assertGet
      benchmark(s"MMAP = false - writing ${keyValues.size} keys") {
        testWrite(map2)
      }
      benchmark(s"MMAP = false - reading ${keyValues.size} keys") {
        testRead(map2)
      }

      val map3 = Maps.memory(4.mb, Accelerator.noBrakes())
      benchmark(s"In-memory - writing ${keyValues.size} keys") {
        testWrite(map3)
      }

      benchmark(s"In-memory - reading ${keyValues.size} keys") {
        testRead(map3)
      }

      map1.close.assertGet
      map2.close.assertGet
      map3.close.assertGet
    }
  }
}
