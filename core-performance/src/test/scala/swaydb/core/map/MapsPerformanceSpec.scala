///*
// * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
// *
// * This file is a part of SwayDB.
// *
// * SwayDB is free software: you can redistribute it and/or modify
// * it under the terms of the GNU Affero General Public License as
// * published by the Free Software Foundation, either version 3 of the
// * License, or (at your option) any later version.
// *
// * SwayDB is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// * GNU Affero General Public License for more details.
// *
// * You should have received a copy of the GNU Affero General Public License
// * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
// */
//
//package swaydb.core.map
//
//import swaydb.IOValues._
//import swaydb.core.CommonAssertions._
//import swaydb.core.TestData._
//import swaydb.core.sweeper.FileSweeper
//import swaydb.core.data.Memory
//import swaydb.core.io.file.Effect
//import swaydb.core.level.zero.LevelZeroSkipListMerger
//import swaydb.core.util.Benchmark
//import swaydb.core.{TestBase, TestSweeper, TestTimer}
//import swaydb.data.accelerate.Accelerator
//import swaydb.data.config.RecoveryMode
//import swaydb.data.order.{KeyOrder, TimeOrder}
//import swaydb.data.slice.Slice


//import swaydb.data.util.StorageUnits._
//
//class MapsPerformanceSpec extends TestBase {
//
//  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default
//  implicit def testTimer: TestTimer = TestTimer.random
//  implicit val fileSweeper: FileSweeper = TestSweeper.fileSweeper
//  implicit val memorySweeper = TestSweeper.memorySweeperMax
//  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
//
//  import swaydb.core.map.serializer.LevelZeroMapEntryReader._
//  import swaydb.core.map.serializer.LevelZeroMapEntryWriter._
//
//  implicit val skipListMerger = LevelZeroSkipListMerger
//
//  "Maps" should {
//    "write key values" in {
//      //      val keyValues = randomIntKeyValues(2000000)
//      val keyValues = randomKeyValues(2000)
//
//      def testWrite(maps: Maps[Slice[Byte], Memory]) =
//        keyValues foreach {
//          keyValue =>
//            maps.write {
//              time =>
//                MapEntry.Put[Slice[Byte], Memory.Put](keyValue.key, Memory.Put(keyValue.key, keyValue.getOrFetchValue, None, time.next))(Level0PutWriter)
//            }.runRandomIO.right.value
//        }
//
//      def testRead(maps: Maps[Slice[Byte], Memory]) =
//        keyValues foreach {
//          keyValue =>
//            maps.get(keyValue.key)
//          //            maps.get(keyValue.key).runIO shouldBe ((ValueType.Add, keyValue.getOrFetchValue.runIO.value))
//        }
//
//      val dir1 = Effect.createDirectoryIfAbsent(testDir.resolve(1.toString))
//
//      val map1 = Maps.persistent[Slice[Byte], Memory](dir1, mmap = true, 4.mb, Accelerator.noBrakes(), RecoveryMode.ReportFailure).runRandomIO.right.value
//      Benchmark(s"MMAP = true - writing ${keyValues.size} keys") {
//        testWrite(map1)
//      }
//      Benchmark(s"MMAP = true - reading ${keyValues.size} keys") {
//        testRead(map1)
//      }
//
//      val dir2 = Effect.createDirectoryIfAbsent(testDir.resolve(2.toString))
//      val map2 = Maps.persistent[Slice[Byte], Memory](dir2, mmap = false, 4.mb, Accelerator.noBrakes(), RecoveryMode.ReportFailure).runRandomIO.right.value
//      Benchmark(s"MMAP = false - writing ${keyValues.size} keys") {
//        testWrite(map2)
//      }
//      Benchmark(s"MMAP = false - reading ${keyValues.size} keys") {
//        testRead(map2)
//      }
//
//      val map3 = Maps.memory(4.mb, Accelerator.noBrakes())
//      Benchmark(s"In-memory - writing ${keyValues.size} keys") {
//        testWrite(map3)
//      }
//
//      Benchmark(s"In-memory - reading ${keyValues.size} keys") {
//        testRead(map3)
//      }
//
//      map1.close.runRandomIO.right.value
//      map2.close.runRandomIO.right.value
//      map3.close.runRandomIO.right.value
//    }
//  }
//}
