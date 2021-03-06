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

import swaydb.IOValues._
import swaydb.core.TestCaseSweeper._
import swaydb.core.segment.Segment
import swaydb.core.util.Benchmark
import swaydb.core.{TestBase, TestCaseSweeper}
import swaydb.data.order.KeyOrder
import swaydb.effect
import swaydb.effect.{Dir, Effect}

import scala.util.Random

class PathsDistributorPerformanceSpec extends TestBase {

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
