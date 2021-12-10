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

package swaydb.core.level

import org.scalamock.scalatest.MockFactory
import org.scalatest.exceptions.TestFailedException
import swaydb.IO
import swaydb.IOValues._
import swaydb.config.MMAP
import swaydb.core.CommonAssertions._
import swaydb.core.CoreTestData._
import swaydb.core.segment.ref.search.ThreadReadState
import swaydb.core.{ACoreSpec, TestSweeper, TestForceSave}
import swaydb.slice.Slice
import swaydb.slice.order.{KeyOrder, TimeOrder}
import swaydb.testkit.RunThis._
import swaydb.utils.OperatingSystem

import scala.util.{Failure, Success, Try}
import swaydb.testkit.TestKit._

class LevelReadSomeSpec0 extends LevelReadSomeSpec

class LevelReadSomeSpec1 extends LevelReadSomeSpec {
  override def levelFoldersCount = 10
  override def mmapSegments = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
  override def level0MMAP = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
  override def appendixStorageMMAP = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
}

class LevelReadSomeSpec2 extends LevelReadSomeSpec {
  override def levelFoldersCount = 10
  override def mmapSegments = MMAP.Off(forceSave = TestForceSave.standard())
  override def level0MMAP = MMAP.Off(forceSave = TestForceSave.standard())
  override def appendixStorageMMAP = MMAP.Off(forceSave = TestForceSave.standard())
}

class LevelReadSomeSpec3 extends LevelReadSomeSpec {
  override def isMemorySpec = true
}

sealed trait LevelReadSomeSpec extends ALevelSpec with MockFactory {

  //  override def deleteFiles = false

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
                      TestSweeper {
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
