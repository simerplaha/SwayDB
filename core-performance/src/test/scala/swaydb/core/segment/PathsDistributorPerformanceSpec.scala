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

package swaydb.core.segment

import swaydb.IOValues._
import swaydb.core.TestCaseSweeper._
import swaydb.core.{ACoreSpec, TestCaseSweeper}
import swaydb.effect.{Dir, Effect}
import swaydb.slice.order.KeyOrder
import swaydb.{effect, Benchmark}
import swaydb.core.level.ALevelSpec

import scala.util.Random

class PathsDistributorPerformanceSpec extends ALevelSpec {

  implicit val keyOrder = KeyOrder.default

  "PathsDistributorPerformanceSpec" in {
    TestCaseSweeper {
      implicit sweeper =>
        val path = createNextLevelPath.sweep()

        //create 5 paths to create randomly distributed Segments in
        val path1 = Effect.createDirectoriesIfAbsent(path.resolve("1"))
        val path2 = Effect.createDirectoriesIfAbsent(path.resolve("2"))
        val path3 = Effect.createDirectoriesIfAbsent(path.resolve("3"))
        val path4 = Effect.createDirectoriesIfAbsent(path.resolve("4"))
        val path5 = Effect.createDirectoriesIfAbsent(path.resolve("5"))

        val paths = Array(path1, path2, path3, path4, path5)

        def randomPath = paths(Math.abs(Random.nextInt(5))).resolve(nextSegmentId)

        //randomly create Segments in different paths to have an un-even distribution in each folder
        def randomlyDistributeSegments(): Iterable[Segment] = {
          val segment = TestSegment(path = randomPath).runRandomIO.right.value
          segment.close.runRandomIO.right.value
          Array.fill(10)(segment)
        }

        val distributor =
          PathsDistributor(
            dirs =
              Seq(
                Dir(path1, 1),
                effect.Dir(path2, 2),
                effect.Dir(path3, 3),
                effect.Dir(path4, 4),
                effect.Dir(path5, 5)
              ),
            segments =
            //for each Segment's request randomly distribute Segments so that there is always un-even distribution
            //of Segments between all 5 folders.
              randomlyDistributeSegments
          )

        //make 10,000 requests to fetch the next path to persist the Segment into.
        //This test does not assert for the result but just the speed.
        //in production scenarios there will never be more then 10 request/seconds, depending on the number of Segments being merged
        //into next Level at one time.
        //These benchmark also include Segment creation in randomlyDistributeSegments which should be accounted for.
        Benchmark("Benchmark PathsDistributor") {
          (1 to 10000) foreach {
            _ =>
              distributor.next
          }
        }
    }
  }
}
