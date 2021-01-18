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
import swaydb.core.TestData._
import swaydb.core.level.zero.LevelZero.LevelZeroMap
import swaydb.core.{TestBase, TestCaseSweeper, TestTimer}
import swaydb.data.MaxKey
import swaydb.data.RunThis._
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

class CompactionLevelZeroTasker_createStacks_Spec extends TestBase with MockFactory {

  implicit val timer = TestTimer.Empty
  implicit val keyOrder = KeyOrder.default
  implicit val timeOrder = TimeOrder.long

  "input.size == 0" in {
    CompactionLevelZeroTasker.createStacks(List.empty) shouldBe empty
  }

  "input.size = 1" in {
    TestCaseSweeper {
      implicit sweeper =>
        val maps: List[LevelZeroMap] =
          List(
            TestMap(randomizedKeyValues(startId = Some(0)))
          )

        val stacks = CompactionLevelZeroTasker.createStacks(maps).asScala

        stacks should have size 1
        stacks.head._1 shouldBe 0.serialise
        stacks.head._2.stack should contain only Left(maps.head)
    }
  }

  "input.size = random but no overlaps" in {
    runThis(20.times, log = true) {
      TestCaseSweeper {
        implicit sweeper =>
          val maps: Iterable[LevelZeroMap] =
            (0 to randomIntMax(10)).foldLeft(ListBuffer.empty[LevelZeroMap]) {
              case (maps, _) =>
                val startId =
                  maps.lastOption match {
                    case Some(last) =>
                      last.cache.lastOptimised.getS.maxKey match {
                        case MaxKey.Fixed(maxKey) =>
                          maxKey.readInt() + 1

                        case MaxKey.Range(_, maxKey) =>
                          maxKey.readInt()
                      }

                    case None =>
                      0
                  }

                maps += TestMap(randomizedKeyValues(startId = Some(startId)))
            }

          val stacks = CompactionLevelZeroTasker.createStacks(maps).asScala
          stacks should have size maps.size
          stacks.values.map(_.stack) shouldBe maps.map(map => ListBuffer(Left(map)))
      }
    }
  }
}
