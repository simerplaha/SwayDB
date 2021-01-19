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
import swaydb.core.CommonAssertions._
import swaydb.core.TestData._
import swaydb.core.data.{KeyValue, Memory}
import swaydb.core.segment.Segment
import swaydb.core.{TestBase, TestCaseSweeper, TestExecutionContext, TestTimer}
import swaydb.data.MaxKey
import swaydb.data.RunThis._
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.serializers.Default._

import scala.collection.mutable.ListBuffer

class CompactionLevelZeroTasker_flatten_Spec extends TestBase with MockFactory {

  implicit val timer = TestTimer.Empty
  implicit val keyOrder = KeyOrder.default
  implicit val timeOrder = TimeOrder.long
  implicit val segmentOrdering = keyOrder.on[Segment](_.minKey)
  implicit val ec = TestExecutionContext.executionContext

  "stack is empty" in {
    runThis(10.times, log = true) {
      TestCaseSweeper {
        implicit sweeper =>
          val updateKeyValues =
            (0 to randomIntMax(20)) map {
              _ =>
                randomizedKeyValues(randomIntMax(100) max 1, startId = Some(0))
            }

          val minId = updateKeyValues.map(_.last.key.readInt()).min

          val putKeyValues =
            (1 to 100).foldLeft(ListBuffer.empty[Memory]) {
              case (buffer, _) =>
                val startId = if (buffer.nonEmpty) buffer.nextKey() else minId
                buffer ++= randomPutKeyValues(eitherOne(randomIntMax(10) max 1, 10), startId = Some(startId))
            } toSlice

          val maps = (updateKeyValues :+ putKeyValues).map(TestMap(_))

          val flattenedMaps = CompactionLevelZeroTasker.flatten(maps.iterator).awaitInf

          //Assert: all maps are in order and there are no overlaps
          flattenedMaps.drop(1).foldLeft(flattenedMaps.head) {
            case (previousCollection, nextCollection) =>
              previousCollection.maxKey match {
                case MaxKey.Fixed(maxKey) =>
                  nextCollection.key.readInt() should be > maxKey.readInt()
                  nextCollection

                case MaxKey.Range(_, maxKey) =>
                  nextCollection.key.readInt() should be >= maxKey.readInt()
                  nextCollection
              }
          }

          def buildExpectedKeyValues(): Iterable[KeyValue] = {
            val level = TestLevel()
            level.put(keyValues = putKeyValues, removeDeletes = true).get
            updateKeyValues.reverse foreach {
              keyValues =>
                level.put(keyValues = keyValues, removeDeletes = true).get
            }
            level.segments().flatMap(_.iterator())
          }

          def buildActualKeyValues() = {
            val level = TestLevel()
            val flattenedKeyValues = flattenedMaps.flatMap(_.iterator()).map(_.toMemory())
            level.put(keyValues = flattenedKeyValues, removeDeletes = true)
            level.segments().flatMap(_.iterator())
          }

          try
            buildActualKeyValues() shouldBe buildExpectedKeyValues()
          catch {
            case throwable: Throwable =>
              throwable.printStackTrace()
              throw throwable
          }
      }
    }
  }
}
