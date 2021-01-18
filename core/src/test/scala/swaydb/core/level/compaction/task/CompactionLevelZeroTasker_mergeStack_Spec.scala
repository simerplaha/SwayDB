/*
 * Copyright (c) 2021 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.core.level.compaction.task

import org.scalamock.scalatest.MockFactory
import org.scalatest.OptionValues._
import swaydb.core.CommonAssertions._
import swaydb.core.TestData._
import swaydb.core.data.Memory
import swaydb.core.level.zero.LevelZero.LevelZeroMap
import swaydb.core.segment.Segment
import swaydb.core.{TestBase, TestCaseSweeper, TestExecutionContext, TestTimer}
import swaydb.data.RunThis._
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._

class CompactionLevelZeroTasker_mergeStack_Spec extends TestBase with MockFactory {

  implicit val timer = TestTimer.Empty
  implicit val keyOrder = KeyOrder.default
  implicit val timeOrder = TimeOrder.long
  implicit val segmentOrdering = keyOrder.on[Segment](_.minKey)
  implicit val ec = TestExecutionContext.executionContext

  "stack is empty" in {
    TestCaseSweeper {
      implicit sweeper =>
        val stack: Iterable[Either[LevelZeroMap, Iterable[Memory]]] =
          List.empty

        CompactionLevelZeroTasker.mergeStack(stack).awaitInf shouldBe Iterable.empty

    }
  }

  "stack.size == 1" in {
    runThis(10.times, log = true) {
      TestCaseSweeper {
        implicit sweeper =>
          val keyValue = Memory.put(1, 1)

          val stack: Iterable[Either[LevelZeroMap, Iterable[Memory]]] =
            List(
              eitherOne(
                Left(TestMap(Slice(keyValue))),
                Right(Slice(keyValue))
              )
            )

          CompactionLevelZeroTasker.mergeStack(stack).awaitInf should contain only keyValue

      }
    }
  }

  "stack.size == 2" in {
    runThis(10.times, log = true) {
      TestCaseSweeper {
        implicit sweeper =>

          val stack: Iterable[Either[LevelZeroMap, Iterable[Memory]]] =
            List(
              Left(TestMap(Slice(Memory.put(1, 2)))),
              Right(Slice(Memory.put(1, 1)))
            )

          CompactionLevelZeroTasker.mergeStack(stack).awaitInf should contain only Memory.put(1, 2)

      }
    }
  }

  "stack.size == 3" in {
    runThis(10.times, log = true) {
      TestCaseSweeper {
        implicit sweeper =>

          val stack: Iterable[Either[LevelZeroMap, Iterable[Memory]]] =
            List(
              Left(TestMap(Slice(Memory.put(1, 2)))),
              Right(Slice(Memory.put(1, 1))),
              Right(Slice(Memory.put(1, 1), Memory.put(2, 2)))
            )

          CompactionLevelZeroTasker.mergeStack(stack).awaitInf should contain only(Memory.put(1, 2), Memory.put(2, 2))
      }
    }
  }


  "random size" in {
    runThis(100.times, log = true) {
      TestCaseSweeper {
        implicit sweeper =>

          val stackedKeyValues =
            (1 to (randomIntMax(10) max 1)) map {
              _ =>
                //ignore pendingApply and ranges so that the order of merge is not important for the outcome.
                randomizedKeyValues(startId = Some(0), count = randomIntMax(100) max 1, addPendingApply = false, addRanges = false, addFunctions = false)
            }

          val expected =
            stackedKeyValues.foldRight(Option.empty[Slice[Memory]]) {
              case (newerKeyValues, mergedKeyValues) =>
                mergedKeyValues match {
                  case Some(olderKeyValues) =>
                    Some(
                      merge(
                        newKeyValues = newerKeyValues,
                        oldKeyValues = olderKeyValues,
                        isLastLevel = false
                      ).toSlice
                    )

                  case None =>
                    Some(newerKeyValues)
                }
            }.value

          val stack: Iterable[Either[LevelZeroMap, Iterable[Memory]]] =
            stackedKeyValues map {
              keyValues =>
                eitherOne(
                  left = Left(TestMap(keyValues)),
                  right = Right(keyValues)
                )
            }

          val actual = CompactionLevelZeroTasker.mergeStack(stack).awaitInf

          actual shouldBe expected
      }
    }
  }
}
