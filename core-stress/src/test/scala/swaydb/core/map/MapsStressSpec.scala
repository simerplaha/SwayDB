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
//
//package swaydb.core.log
//
//import org.scalatest.OptionValues._
//import swaydb.IOValues._
//import swaydb.core.CommonAssertions._
//import swaydb.core.TestData._
//import swaydb.core.sweeper.FileSweeper
//import swaydb.core.data.Memory
//import swaydb.core.io.file.Effect
//import swaydb.core.io.file.Effect._
//import swaydb.core.level.zero.LevelZeroSkipListMerger
//import swaydb.core.{TestBase, TestSweeper, TestTimer}
//import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
//import swaydb.data.config.RecoveryMode
//import swaydb.slice.order.{KeyOrder, TimeOrder}
//import swaydb.slice.Slice
//import swaydb.data.util.StorageUnits._
//
//class MapsStressSpec extends TestBase {
//
//  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default
//  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
//  implicit def testTimer: TestTimer = TestTimer.Empty
//  implicit val fileSweeper: FileSweeper = TestSweeper.fileSweeper
//  implicit val memorySweeper = TestSweeper.memorySweeperMax
//
//  import swaydb.core.log.serializer.LevelZeroLogEntryReader._
//  import swaydb.core.log.serializer.LevelZeroLogEntryWriter._
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
//          Accelerator(meter.currentLogSize, None)
//        }
//
//      def testWrite(mmapPersistentLevelAppendixLogs Maps[Slice[Byte], Memory]) = {
//        keyValues foreach {
//          keyValue =>
//            maps.write(time => LogEntry.Put(keyValue.key, Memory.Put(keyValue.key, keyValue.getOrFetchValue, None, time.next))).runRandomIO.right.value
//        }
//      }
//
//      def testRead(mmapPersistentLevelAppendixLogs Maps[Slice[Byte], Memory]) = {
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
