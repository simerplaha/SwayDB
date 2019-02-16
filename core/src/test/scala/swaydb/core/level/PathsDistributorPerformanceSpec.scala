/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.level

import swaydb.core.TestBase
import swaydb.core.segment.Segment
import swaydb.core.util.Benchmark
import swaydb.data.config.Dir
import swaydb.data.order.KeyOrder
import swaydb.core.TestData._
import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import swaydb.core.IOAssert._
import scala.util.Random
import swaydb.core.io.file.IOEffect

class PathsDistributorPerformanceSpec extends TestBase with Benchmark {

  implicit val keyOrder = KeyOrder.default

  "PathsDistributorPerformanceSpec" in {
    val path = createNextLevelPath

    //create 5 paths to create randomly distributed Segments in
    val path1 = IOEffect.createDirectoriesIfAbsent(path.resolve("1"))
    val path2 = IOEffect.createDirectoriesIfAbsent(path.resolve("2"))
    val path3 = IOEffect.createDirectoriesIfAbsent(path.resolve("3"))
    val path4 = IOEffect.createDirectoriesIfAbsent(path.resolve("4"))
    val path5 = IOEffect.createDirectoriesIfAbsent(path.resolve("5"))

    val paths = Array(path1, path2, path3, path4, path5)

    def randomPath = paths(Math.abs(Random.nextInt(5))).resolve(nextSegmentId)

    //randomly create Segments in different paths to have an un-even distribution in each folder
    def randomlyDistributeSegments(): Iterable[Segment] = {
      val segment = TestSegment(path = randomPath).assertGet
      segment.close.assertGet
      Array.fill(10)(segment)
    }

    val distributor =
      PathsDistributor(
        dirs =
          Seq(
            Dir(path1, 1),
            Dir(path2, 2),
            Dir(path3, 3),
            Dir(path4, 4),
            Dir(path5, 5)
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
    benchmark("Benchmark PathsDistributor") {
      (1 to 10000) foreach {
        _ =>
          distributor.next
      }
    }
  }
}
