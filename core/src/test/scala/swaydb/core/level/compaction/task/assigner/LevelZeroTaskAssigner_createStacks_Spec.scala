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

package swaydb.core.level.compaction.task.assigner

import org.scalamock.scalatest.MockFactory
import swaydb.core.TestData._
import swaydb.core.level.zero.LevelZero.LevelZeroMap
import swaydb.core.{TestBase, TestCaseSweeper, TestTimer}
import swaydb.data.MaxKey
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.testkit.RunThis._

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

class LevelZeroTaskAssigner_createStacks_Spec extends TestBase with MockFactory {

  implicit val timer = TestTimer.Empty
  implicit val keyOrder = KeyOrder.default
  implicit val timeOrder = TimeOrder.long

  "input.size == 0" in {
    LevelZeroTaskAssigner.createStacks(List.empty) shouldBe empty
  }

  "input.size = 1" in {
    TestCaseSweeper {
      implicit sweeper =>
        val maps: List[LevelZeroMap] =
          List(
            TestMap(randomizedKeyValues(startId = Some(0)))
          )

        val stacks = LevelZeroTaskAssigner.createStacks(maps).asScala

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

          val stacks = LevelZeroTaskAssigner.createStacks(maps).asScala
          stacks should have size maps.size
          stacks.values.map(_.stack) shouldBe maps.map(map => ListBuffer(Left(map)))
      }
    }
  }
}
