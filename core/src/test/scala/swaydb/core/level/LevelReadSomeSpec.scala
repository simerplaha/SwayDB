/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.core.level

import org.scalamock.scalatest.MockFactory
import org.scalatest.exceptions.TestFailedException
import swaydb.IO
import swaydb.IOValues._
import swaydb.core.CommonAssertions._
import swaydb.core.TestData._
import swaydb.core.segment.ref.search.ThreadReadState
import swaydb.core.{TestBase, TestCaseSweeper, TestForceSave}
import swaydb.data.compaction.CompactionConfig.CompactionParallelism
import swaydb.testkit.RunThis._
import swaydb.data.config.MMAP
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice

import scala.util.{Failure, Success, Try}
import swaydb.utils.OperatingSystem
import swaydb.utils.OperatingSystem._
import swaydb.utils.FiniteDurations._
import swaydb.utils.StorageUnits._

class LevelReadSomeSpec0 extends LevelReadSomeSpec

class LevelReadSomeSpec1 extends LevelReadSomeSpec {
  override def levelFoldersCount = 10
  override def mmapSegments = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
  override def level0MMAP = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
  override def appendixStorageMMAP = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
}

class LevelReadSomeSpec2 extends LevelReadSomeSpec {
  override def levelFoldersCount = 10
  override def mmapSegments = MMAP.Off(forceSave = TestForceSave.channel())
  override def level0MMAP = MMAP.Off(forceSave = TestForceSave.channel())
  override def appendixStorageMMAP = MMAP.Off(forceSave = TestForceSave.channel())
}

class LevelReadSomeSpec3 extends LevelReadSomeSpec {
  override def inMemoryStorage = true
}

sealed trait LevelReadSomeSpec extends TestBase with MockFactory {

  //  override def deleteFiles = false
  implicit val compactionParallelism: CompactionParallelism = CompactionParallelism.availableProcessors()

  val keyValuesCount = 100

  val times = 10

  "return Put" when {

    "level has valid puts" in {
      runThis(times) {
        assertLevel(
          level0KeyValues =
            (_, _, testTimer) =>
              randomPutKeyValues(keyValuesCount)(testTimer),

          assertLevel0 =
            (level0KeyValues, _, _, level) =>
              assertReads(level0KeyValues, level)
        )
      }
    }

    "contains put that were updated" in {
      runThis(times) {
        val updatedValue = randomStringSliceOptional
        //also update the deadline so that no puts are expired
        val updatedDeadline = eitherOne(None, randomDeadlineOption(false))

        assertLevel(
          level0KeyValues =
            (level1KeyValues, level2KeyValues, testTimer) => {
              val puts = unexpiredPuts(level2KeyValues ++ level1KeyValues)
              randomUpdate(puts, updatedValue, updatedDeadline, false)(testTimer)
            },

          level1KeyValues =
            (level2KeyValues, testTimer) =>
              randomizedKeyValues(keyValuesCount, startId = Some(maxKey(Slice(level2KeyValues.last)).maxKey.readInt() + 10000))(testTimer),

          level2KeyValues =
            testTimer =>
              randomizedKeyValues(keyValuesCount, startId = Some(0))(testTimer),

          assertLevel0 =
            (level0KeyValues, level1KeyValues, level2KeyValues, level) =>
              level0KeyValues foreach {
                update =>
                  val (gotValue, gotDeadline) =
                    level.get(update.key, ThreadReadState.random).toOptionPut.runRandomIO.map {
                      case Some(put) =>
                        val value = put.getOrFetchValue.runRandomIO.right.value
                        (value, put.deadline)

                      case None =>
                        (None, None)
                    }.runRandomIO.right.value

                  Try(gotValue shouldBe updatedValue) match {
                    case Failure(testException: TestFailedException) =>
                      //if test failed check merging all key-values result in the key returning none.
                      implicit val keyOrder = KeyOrder.default
                      implicit val timeOrder = TimeOrder.long
                      TestCaseSweeper {
                        implicit sweeper =>
                          import sweeper._

                          val level: Level = TestLevel()
                          level.put(level2KeyValues).runRandomIO.right.value
                          level.put(level1KeyValues).runRandomIO.right.value
                          level.put(level0KeyValues).runRandomIO.right.value

                          //if after merging into a single Level the result is not empty then print all the failed exceptions.
                          Try(IO.Defer(level.get(update.key, ThreadReadState.random).toOptionPut).runIO.runRandomIO.right.value shouldBe empty).failed foreach {
                            exception =>
                              exception.printStackTrace()
                              throw testException
                          }
                      }

                    case Failure(exception) =>
                      throw exception

                    case Success(_) =>
                      //on successful value check deadline is updated.
                      updatedDeadline foreach {
                        updatedDeadline =>
                          gotDeadline should contain(updatedDeadline)
                      }
                  }
              }
        )
      }
    }
  }
}
