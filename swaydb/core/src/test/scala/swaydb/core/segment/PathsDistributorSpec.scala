///*
// * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package swaydb.core.segment
//
//import org.scalamock.scalatest.MockFactory
//import swaydb.effect.IOValues._
//import swaydb.core.TestSweeper._
//import swaydb.core.{ACoreSpec, TestSweeper}
//import swaydb.core.level.ALevelSpec
//import swaydb.effect
//import swaydb.effect.{Dir, Effect}
//import swaydb.slice.order.KeyOrder
//import swaydb.testkit.RunThis._
//
//import java.nio.file.{Path, Paths}
//
//class PathsDistributorSpec extends ALevelSpec with MockFactory {
//
//  implicit val keyOrder = KeyOrder.default
//
//  implicit def stringToPath(string: String): Path = Paths.get(string)
//
//  "Distributor.getDistributions" should {
//    "return distributions and total Segments count when Segments are empty" in {
//      val dirs = Seq(Dir("1", 1), Dir("2", 2), Dir("3", 3))
//
//      val (distributions, segmentsCount) = PathsDistributor.getDistributions(dirs, () => Seq())
//      distributions shouldBe Array(Distribution("1", 1, 0, 0), Distribution("2", 2, 0, 0), Distribution("3", 3, 0, 0))
//      segmentsCount shouldBe 0
//    }
//
//    "return distributions and total Segments count when Segments are non-empty" in {
//      TestSweeper {
//        implicit sweeper =>
//          val path = createNextLevelPath.sweep()
//          val path1 = Effect.createDirectoriesIfAbsent(path.resolve("1"))
//          val path2 = Effect.createDirectoriesIfAbsent(path.resolve("2"))
//          val path3 = Effect.createDirectoriesIfAbsent(path.resolve("3"))
//
//          val dirs =
//            Seq(
//              Dir(path1, 1),
//              effect.Dir(path2, 2),
//              effect.Dir(path3, 3)
//            )
//
//          val (distributions, segmentsCount) =
//            PathsDistributor.getDistributions(
//              dirs,
//              () =>
//                Seq(
//                  TestSegment(path = path1.resolve("11.seg")).runRandomIO.get,
//                  TestSegment(path = path3.resolve("31.seg")).runRandomIO.get,
//                  TestSegment(path = path3.resolve("32.seg")).runRandomIO.get
//                )
//            )
//
//          distributions shouldBe
//            Array(
//              Distribution(path = path1, distributionRatio = 1, actualSize = 1, expectedSize = 0),
//              Distribution(path = path2, distributionRatio = 2, actualSize = 0, expectedSize = 0),
//              Distribution(path = path3, distributionRatio = 3, actualSize = 2, expectedSize = 0)
//            )
//          segmentsCount shouldBe 3
//      }
//    }
//  }
//
//  "Distributor.distribute" should {
//    "return distributions the same distributions without any changes if the size == 0" in {
//      val distributions =
//        Array(
//          Distribution(path = "1", distributionRatio = 1, actualSize = 0, expectedSize = 0),
//          Distribution(path = "2", distributionRatio = 2, actualSize = 0, expectedSize = 0),
//          Distribution(path = "3", distributionRatio = 3, actualSize = 0, expectedSize = 0),
//          Distribution(path = "4", distributionRatio = 3, actualSize = 0, expectedSize = 0),
//          Distribution(path = "5", distributionRatio = 3, actualSize = 0, expectedSize = 0)
//        )
//
//      PathsDistributor.distribute(0, distributions) shouldBe distributions
//    }
//
//    "return distributions with expectedSizes when actualSize == 0" in {
//      val distributions =
//        Array(
//          Distribution(path = "1", distributionRatio = 1, actualSize = 0, expectedSize = 0),
//          Distribution(path = "2", distributionRatio = 2, actualSize = 0, expectedSize = 0),
//          Distribution(path = "3", distributionRatio = 3, actualSize = 0, expectedSize = 0),
//          Distribution(path = "4", distributionRatio = 4, actualSize = 0, expectedSize = 0),
//          Distribution(path = "5", distributionRatio = 5, actualSize = 0, expectedSize = 0)
//        )
//
//      PathsDistributor.distribute(100, distributions) shouldBe
//        Array(
//          Distribution(path = "5", distributionRatio = 5, actualSize = 0, expectedSize = 30),
//          Distribution(path = "4", distributionRatio = 4, actualSize = 0, expectedSize = 28),
//          Distribution(path = "3", distributionRatio = 3, actualSize = 0, expectedSize = 21),
//          Distribution(path = "2", distributionRatio = 2, actualSize = 0, expectedSize = 14),
//          Distribution(path = "1", distributionRatio = 1, actualSize = 0, expectedSize = 7)
//        )
//    }
//
//    "return distributions with expectedSizes when actualSize is non 0" in {
//      val distributions =
//        Array(
//          Distribution(path = "1", distributionRatio = 1, actualSize = 3, expectedSize = 0),
//          Distribution(path = "2", distributionRatio = 2, actualSize = 4, expectedSize = 0),
//          Distribution(path = "3", distributionRatio = 3, actualSize = 5, expectedSize = 0),
//          Distribution(path = "4", distributionRatio = 4, actualSize = 6, expectedSize = 0),
//          Distribution(path = "5", distributionRatio = 5, actualSize = 7, expectedSize = 0)
//        )
//
//      PathsDistributor.distribute(100, distributions) shouldBe
//        Array(
//          Distribution(path = "5", distributionRatio = 5, actualSize = 7, expectedSize = 30),
//          Distribution(path = "4", distributionRatio = 4, actualSize = 6, expectedSize = 28),
//          Distribution(path = "3", distributionRatio = 3, actualSize = 5, expectedSize = 21),
//          Distribution(path = "2", distributionRatio = 2, actualSize = 4, expectedSize = 14),
//          Distribution(path = "1", distributionRatio = 1, actualSize = 3, expectedSize = 7)
//        )
//    }
//
//    "return distributions with largest missing Segments the at the head" in {
//      val distributions =
//        Array(
//          Distribution(path = "1", distributionRatio = 1, actualSize = 29, expectedSize = 0),
//          Distribution(path = "2", distributionRatio = 2, actualSize = 22, expectedSize = 0),
//          Distribution(path = "3", distributionRatio = 3, actualSize = 1, expectedSize = 0),
//          Distribution(path = "4", distributionRatio = 4, actualSize = 14, expectedSize = 0),
//          Distribution(path = "5", distributionRatio = 5, actualSize = 30, expectedSize = 0)
//        )
//
//      PathsDistributor.distribute(100, distributions) shouldBe
//        Array(
//          Distribution(path = "3", distributionRatio = 3, actualSize = 1, expectedSize = 21), //20 missing
//          Distribution(path = "4", distributionRatio = 4, actualSize = 14, expectedSize = 28), //12 missing
//          Distribution(path = "5", distributionRatio = 5, actualSize = 30, expectedSize = 30), //exact
//          Distribution(path = "2", distributionRatio = 2, actualSize = 22, expectedSize = 14), // -8 overflow
//          Distribution(path = "1", distributionRatio = 1, actualSize = 29, expectedSize = 7) //-22 overflow
//        )
//    }
//  }
//
//  "PathsDistributor.next" should {
//    "always returns the first path if there is only 1 directory" in {
//      val distributor = PathsDistributor(Seq(Dir("1", 0)), () => Seq.empty)
//
//      (1 to 100) foreach {
//        _ =>
//          distributor.next() shouldBe ("1": Path)
//      }
//    }
//
//    "return paths based on the distribution ratio" in {
//
//      val segments = mockFunction[Iterable[Segment]]
//      segments.expects() returning Seq() repeat 100.times
//
//      val distributor =
//        PathsDistributor(
//          Seq(
//            Dir("1", 1),
//            Dir("2", 2),
//            Dir("3", 3)
//          ),
//          segments
//        )
//
//      distributor.queuedPaths.toList shouldBe List[Path]("1", "2", "2", "3", "3", "3")
//
//      //run a total of 100 requests.
//      (1 to 100) foreach {
//        _ =>
//          //expect 6 calls which would equally return the next path from the queue. After the last a new queue is started.
//          (1 to 6) foreach {
//            i =>
//              val next = distributor.next()
//              if (i == 6 || i == 5 || i == 4) {
//                next shouldBe ("3": Path)
//              } else if (i == 2 || i == 3) {
//                next shouldBe ("2": Path)
//              } else {
//                next shouldBe ("1": Path)
//              }
//          }
//      }
//    }
//
//    "return paths based on the distribution ratio and update distribution when Segments are distributed unevenly" in {
//      TestSweeper {
//        implicit sweeper =>
//          val path = createNextLevelPath.sweep()
//          val path1 = Effect.createDirectoriesIfAbsent(path.resolve("1"))
//          val path2 = Effect.createDirectoriesIfAbsent(path.resolve("2"))
//          val path3 = Effect.createDirectoriesIfAbsent(path.resolve("3"))
//
//          val segments = mockFunction[Iterable[Segment]]
//          segments.expects() returning Seq()
//
//          val distributor =
//            PathsDistributor(
//              Seq(
//                effect.Dir(path1, 1),
//                effect.Dir(path2, 2),
//                effect.Dir(path3, 3)
//              ),
//              segments
//            )
//
//          distributor.queuedPaths.toList shouldBe List[Path](path1, path2, path2, path3, path3, path3)
//
//          //first batch
//          distributor.next() shouldBe path1
//          distributor.next() shouldBe path2
//          distributor.next() shouldBe path2
//          distributor.next() shouldBe path3
//          distributor.next() shouldBe path3
//          distributor.next() shouldBe path3
//
//          //second batch, where each Path has 1 Segment = a total of 3 Segments. Distribution ratio for path2 is 2 and but it contains only 1 Segment.
//          segments.expects() returning
//            Seq(
//              TestSegment(path = path1.resolve("1.seg")).runRandomIO.get,
//              TestSegment(path = path2.resolve("2.seg")).runRandomIO.get,
//              TestSegment(path = path3.resolve("3.seg")).runRandomIO.get
//            )
//          //a total of 3 Segments but path2 was expected to have 2 Segments which it does not so the distribution returns path2 to be filled.
//          distributor.next() shouldBe path2
//
//          //third batch. Distribution is fixed, goes back to returning normal paths based on the default distribution ratio.
//          segments.expects() returning Seq()
//          distributor.next() shouldBe path1
//          distributor.next() shouldBe path2
//          distributor.next() shouldBe path2
//          distributor.next() shouldBe path3
//          distributor.next() shouldBe path3
//          distributor.next() shouldBe path3
//
//          //4ht batch, path3 contains none but path1 and path2 have one Segment.
//          segments.expects() returning
//            Seq(
//              TestSegment(path = path1.resolve("11.seg")).runRandomIO.get,
//              TestSegment(path = path2.resolve("22.seg")).runRandomIO.get,
//              TestSegment(path = path2.resolve("222.seg")).runRandomIO.get,
//              TestSegment(path = path2.resolve("2222.seg")).runRandomIO.get,
//              TestSegment(path = path2.resolve("22222.seg")).runRandomIO.get,
//              TestSegment(path = path2.resolve("222222.seg")).runRandomIO.get
//            )
//          //a total 6 Segments which path3 is empty, here the path3 gets filled before going back to normal
//          distributor.next() shouldBe path3
//          distributor.next() shouldBe path3
//          distributor.next() shouldBe path3
//
//          //5th batch. Distribution is fixed all Paths contains equally distributed Segments based on distribution ration.
//          segments.expects() returning
//            Seq(
//              TestSegment(path = path1.resolve("111.seg")).runRandomIO.get,
//              TestSegment(path = path2.resolve("2222222.seg")).runRandomIO.get,
//              TestSegment(path = path2.resolve("22222222.seg")).runRandomIO.get,
//              TestSegment(path = path3.resolve("33.seg")).runRandomIO.get,
//              TestSegment(path = path3.resolve("333.seg")).runRandomIO.get,
//              TestSegment(path = path3.resolve("3333.seg")).runRandomIO.get
//            )
//          // goes back to returning normal paths based on the default distribution ratio.
//          distributor.next() shouldBe path1
//          distributor.next() shouldBe path2
//          distributor.next() shouldBe path2
//          distributor.next() shouldBe path3
//          distributor.next() shouldBe path3
//          distributor.next() shouldBe path3
//      }
//    }
//  }
//}
