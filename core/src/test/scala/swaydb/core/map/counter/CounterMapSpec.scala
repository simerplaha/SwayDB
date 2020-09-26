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
 * If you modify this Program, or any covered work, by linking or combining
 * it with other code, such other code is not for that reason alone subject
 * to any of the requirements of the GNU Affero GPL version 3.
 */

package swaydb.core.map.counter

import swaydb.IOValues._
import swaydb.core.TestCaseSweeper._
import swaydb.core.TestData._
import swaydb.core.{TestBase, _}
import swaydb.core.map.MapTestUtil._
import swaydb.core.map.serializer._
import swaydb.data.config.MMAP
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.data.RunThis._

import scala.collection.mutable.ListBuffer

class CounterMapSpec extends TestBase {

  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default

  implicit val functionsEntryWriter = CounterMapEntryWriter.CounterPutMapEntryWriter
  implicit val functionsEntryReader = CounterMapEntryReader.CounterPutMapEntryReader

  "nextCommit" should {
    "reserve greater startId" when {
      "startId is greater than mod" in {
        PersistentCounterMap.nextCommit(mod = 1, startId = 0) shouldBe 1
        PersistentCounterMap.nextCommit(mod = 1, startId = 2) shouldBe 3

        PersistentCounterMap.nextCommit(mod = 5, startId = 4) shouldBe 5
        PersistentCounterMap.nextCommit(mod = 5, startId = 6) shouldBe 10
        PersistentCounterMap.nextCommit(mod = 5, startId = 7) shouldBe 10
        PersistentCounterMap.nextCommit(mod = 5, startId = 8) shouldBe 10
        PersistentCounterMap.nextCommit(mod = 5, startId = 9) shouldBe 10
        PersistentCounterMap.nextCommit(mod = 5, startId = 10) shouldBe 15
        PersistentCounterMap.nextCommit(mod = 5, startId = 11) shouldBe 15
        PersistentCounterMap.nextCommit(mod = 5, startId = 12) shouldBe 15
        PersistentCounterMap.nextCommit(mod = 5, startId = 13) shouldBe 15
        PersistentCounterMap.nextCommit(mod = 5, startId = 14) shouldBe 15
        PersistentCounterMap.nextCommit(mod = 5, startId = 15) shouldBe 20
      }
    }
  }

  "fetch the next long" in {
    runThis(10.times, log = true) {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._

          val mod = randomIntMax(10) max 1

          val map =
            CounterMap.persistent(
              dir = randomDir,
              fileSize = randomIntMax(100) max 1,
              mmap = MMAP.randomForMap(),
              mod = mod
            ).value.sweep()

          val expectedNext = CounterMap.startId + 1
          map.next shouldBe expectedNext
          map.next shouldBe expectedNext + 1
          map.next shouldBe expectedNext + 2

          val reopened = map.reopen
          val startId = reopened.startId
          startId should be > (expectedNext + 2)
          reopened.next should be > startId
      }
    }
  }

  "initialise and reopen" in {
    runThis(10.times, log = true) {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._

          //random mods and iterations
          val mod = randomNextInt(100) max 1
          val maxIteration = randomIntMax(10000) max 1

          val usedIds = ListBuffer.empty[Long]

          val map =
            CounterMap.persistent(
              dir = randomDir,
              fileSize = randomIntMax(1.kb) max 1,
              mmap = MMAP.randomForMap(),
              mod = mod
            ).value.sweep()

          val expectedStart = CounterMap.startId + 1
          val expectedLast = expectedStart + maxIteration
          (expectedStart to expectedLast) foreach {
            i =>
              map.next shouldBe i
              usedIds += i
          }

          //reopening should result in startId greater than last fetched
          val opened = map.reopen
          opened.startId should be > expectedLast

          //randomly reopen and fetch next and store the next long.
          (1 to 100).foldLeft(opened) {
            case (opened, _) =>
              val next = opened.next
              usedIds += next

              if (randomBoolean())
                opened.reopen
              else
                opened
          }

          //no duplicate
          usedIds.distinct.size shouldBe usedIds.size

          //should be increment order
          usedIds.foldLeft(Long.MinValue) {
            case (previous, next) =>
              previous should be < next
              next
          }
      }
    }
  }
}
