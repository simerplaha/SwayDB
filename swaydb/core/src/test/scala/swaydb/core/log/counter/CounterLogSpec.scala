///*
// * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package swaydb.core.log.counter
//
//import swaydb.effect.IOValues._
//import swaydb.config.MMAP
//import swaydb.core._
//import swaydb.core.CoreTestData._
//import swaydb.core.TestSweeper._
//import swaydb.core.log.LogTestUtil._
//import swaydb.core.log.serialiser._
//import swaydb.core.log.ALogSpec
//import swaydb.slice.Slice
//import swaydb.slice.order.KeyOrder
//import swaydb.testkit.RunThis._
//import swaydb.testkit.TestKit._
//import swaydb.utils.StorageUnits._
//
//import scala.collection.mutable.ListBuffer
//import swaydb.core.file.CoreFileTestKit._
//
//class CounterLogSpec extends ALogSpec {
//
//  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default
//
//  implicit val functionsEntryWriter = CounterLogEntryWriter.CounterPutLogEntryWriter
//  implicit val functionsEntryReader = CounterLogEntryReader.CounterPutLogEntryReader
//
//  "nextCommit" should {
//    "reserve greater startId" when {
//      "startId is greater than mod" in {
//        PersistentCounterLog.nextCommit(mod = 1, startId = 0) shouldBe 1
//        PersistentCounterLog.nextCommit(mod = 1, startId = 2) shouldBe 3
//
//        PersistentCounterLog.nextCommit(mod = 5, startId = 4) shouldBe 5
//        PersistentCounterLog.nextCommit(mod = 5, startId = 6) shouldBe 10
//        PersistentCounterLog.nextCommit(mod = 5, startId = 7) shouldBe 10
//        PersistentCounterLog.nextCommit(mod = 5, startId = 8) shouldBe 10
//        PersistentCounterLog.nextCommit(mod = 5, startId = 9) shouldBe 10
//        PersistentCounterLog.nextCommit(mod = 5, startId = 10) shouldBe 15
//        PersistentCounterLog.nextCommit(mod = 5, startId = 11) shouldBe 15
//        PersistentCounterLog.nextCommit(mod = 5, startId = 12) shouldBe 15
//        PersistentCounterLog.nextCommit(mod = 5, startId = 13) shouldBe 15
//        PersistentCounterLog.nextCommit(mod = 5, startId = 14) shouldBe 15
//        PersistentCounterLog.nextCommit(mod = 5, startId = 15) shouldBe 20
//      }
//    }
//  }
//
//  "fetch the next long" in {
//    runThis(10.times, log = true) {
//      TestSweeper {
//        implicit sweeper =>
//          import sweeper._
//
//          val mod = randomIntMax(10) max 1
//
//          val map =
//            CounterLog.persistent(
//              dir = randomDir(),
//              fileSize = randomIntMax(100) max 1,
//              mmap = MMAP.randomForLog(),
//              mod = mod
//            ).value.sweep()
//
//          val expectedNext = CounterLog.startId + 1
//          map.next shouldBe expectedNext
//          map.next shouldBe expectedNext + 1
//          map.next shouldBe expectedNext + 2
//
//          val reopened = map.reopen
//          val startId = reopened.startId
//          startId should be > (expectedNext + 2)
//          reopened.next should be > startId
//      }
//    }
//  }
//
//  "initialise and reopen" in {
//    runThis(10.times, log = true) {
//      TestSweeper {
//        implicit sweeper =>
//          import sweeper._
//
//          //random mods and iterations
//          val mod = randomNextInt(100) max 1
//          val maxIteration = randomIntMax(10000) max 1
//
//          val usedIds = ListBuffer.empty[Long]
//
//          val map =
//            CounterLog.persistent(
//              dir = randomDir(),
//              fileSize = randomIntMax(1.kb) max 1,
//              mmap = MMAP.randomForLog(),
//              mod = mod
//            ).value.sweep()
//
//          val expectedStart = CounterLog.startId + 1
//          val expectedLast = expectedStart + maxIteration
//          (expectedStart to expectedLast) foreach {
//            i =>
//              map.next shouldBe i
//              usedIds += i
//          }
//
//          //reopening should result in startId greater than last fetched
//          val opened = map.reopen
//          opened.startId should be > expectedLast
//
//          //randomly reopen and fetch next and store the next long.
//          (1 to 100).foldLeft(opened) {
//            case (opened, _) =>
//              val next = opened.next
//              usedIds += next
//
//              if (randomBoolean())
//                opened.reopen
//              else
//                opened
//          }
//
//          //no duplicate
//          usedIds.distinct.size shouldBe usedIds.size
//
//          //should be increment order
//          usedIds.foldLeft(Long.MinValue) {
//            case (previous, next) =>
//              previous should be < next
//              next
//          }
//      }
//    }
//  }
//}