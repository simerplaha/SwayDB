///*
// * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
//import org.scalatest.OptionValues._
//import swaydb.IOValues._
//import swaydb.core.CommonAssertions._
//import swaydb.core.TestData._
//import swaydb.core.actor.FileSweeper
//import swaydb.core.data.Memory
//import swaydb.core.io.file.Effect
//import swaydb.core.io.file.Effect._
//import swaydb.core.level.zero.LevelZeroSkipListMerger
//import swaydb.core.{TestBase, TestSweeper, TestTimer}
//import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
//import swaydb.data.config.RecoveryMode
//import swaydb.data.order.{KeyOrder, TimeOrder}
//import swaydb.data.slice.Slice
//import swaydb.data.util.StorageUnits._
//
//class MapsStressSpec extends TestBase {
//
//  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default
//  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
//  implicit def testTimer: TestTimer = TestTimer.Empty
//  implicit val fileSweeper: FileSweeper.Enabled = TestSweeper.fileSweeper
//  implicit val memorySweeper = TestSweeper.memorySweeperMax
//
//  import swaydb.core.map.serializer.LevelZeroMapEntryReader._
//  import swaydb.core.map.serializer.LevelZeroMapEntryWriter._
//
//  implicit val skipListMerger = LevelZeroSkipListMerger
//
//  val keyValueCount = 100
//
//  "Maps.persistent" should {
//    "initialise and recover over 1000 maps persistent map and on reopening them should recover state all 1000 persisted maps" in {
//      val keyValues = randomKeyValues(keyValueCount)
//
//      //disable braking
//      val acceleration =
//        (meter: LevelZeroMeter) => {
//          Accelerator(meter.currentMapSize, None)
//        }
//
//      def testWrite(maps: Maps[Slice[Byte], Memory]) = {
//        keyValues foreach {
//          keyValue =>
//            maps.write(time => MapEntry.Put(keyValue.key, Memory.Put(keyValue.key, keyValue.getOrFetchValue, None, time.next))).runRandomIO.right.value
//        }
//      }
//
//      def testRead(maps: Maps[Slice[Byte], Memory]) = {
//        keyValues foreach {
//          keyValue =>
//            val got = maps.get(keyValue.key).value
//            got.isInstanceOf[Memory.Put] shouldBe true
//            got.key shouldBe keyValue.key
//            got.getOrFetchValue shouldBe keyValue.getOrFetchValue
//        }
//      }
//
//      val dir1 = Effect.createDirectoryIfAbsent(testDir.resolve(1.toString))
//      val dir2 = Effect.createDirectoryIfAbsent(testDir.resolve(2.toString))
//
//      val map1 = Maps.persistent[Slice[Byte], Memory](dir1, mmap = true, 1.byte, acceleration, RecoveryMode.ReportFailure).runRandomIO.right.value
//      testWrite(map1)
//      testRead(map1)
//
//      val map2 = Maps.persistent[Slice[Byte], Memory](dir2, mmap = true, 1.byte, acceleration, RecoveryMode.ReportFailure).runRandomIO.right.value
//      testWrite(map2)
//      testRead(map2)
//
//      val map3 = Maps.memory(1.byte, acceleration)
//      testWrite(map3)
//      testRead(map3)
//
//      def reopen = {
//        val open1 = Maps.persistent[Slice[Byte], Memory](dir1, mmap = false, 1.byte, acceleration, RecoveryMode.ReportFailure).runRandomIO.right.value
//        testRead(open1)
//        val open2 = Maps.persistent[Slice[Byte], Memory](dir2, mmap = true, 1.byte, acceleration, RecoveryMode.ReportFailure).runRandomIO.right.value
//        testRead(open2)
//
//        open1.close.runRandomIO.right.value
//        open2.close.runRandomIO.right.value
//      }
//
//      reopen
//      reopen //reopen again
//
//      map1.close.runRandomIO.right.value
//      map2.close.runRandomIO.right.value
//      map2.close.runRandomIO.right.value
//
//      println("total number of maps recovered: " + dir1.folders.size)
//    }
//  }
//}
