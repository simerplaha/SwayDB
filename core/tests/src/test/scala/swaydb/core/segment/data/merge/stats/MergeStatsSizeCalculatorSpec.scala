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
//package swaydb.core.segment.data.merge.stats
//
//import org.scalamock.scalatest.MockFactory
//import org.scalatest.EitherValues
//import swaydb.core.CoreTestData._
//import swaydb.core.segment.block.segment.SegmentBlockConfig
//import swaydb.core.segment.block.sortedindex.SortedIndexBlockConfig
//import swaydb.core.segment.data.Memory
//import swaydb.core.{ACoreSpec, TestExecutionContext, TestTimer}
//import swaydb.core.segment.ASegmentSpec
//import swaydb.serializers.Default._
//import swaydb.serializers._
//import swaydb.testkit.RunThis._
//import swaydb.testkit.TestKit._
//
//class MergeStatsSizeCalculatorSpec extends ASegmentSpec with MockFactory with EitherValues {
//
//  implicit val ec = TestExecutionContext.executionContext
//  implicit val timer = TestTimer.Empty
//
//  "isStatsSmall" should {
//    "return false" when {
//
//      "stats is null" in {
//        implicit val sortedIndexConfig: SortedIndexBlockConfig = SortedIndexBlockConfig.random
//        implicit val segmentConfig: SegmentBlockConfig = SegmentBlockConfig.random
//
//        MergeStatsSizeCalculator.persistentSizeCalculator.isStatsOrNullSmall(statsOrNull = null) shouldBe false
//      }
//
//      "segmentSize and maxCount exceed limit" in {
//        runThis(100.times, log = true) {
//
//          val stats = MergeStatsCreator.PersistentCreator.create(randomBoolean())
//          stats.addOne(Memory.put(1, 1))
//          stats.addOne(Memory.put(2, 2))
//          stats.addOne(Memory.put(3, 3))
//          stats.addOne(Memory.put(4, 4))
//
//          implicit val sortedIndexConfig: SortedIndexBlockConfig = SortedIndexBlockConfig.random
//
//          val closedStats =
//            stats.close(
//              hasAccessPositionIndex = sortedIndexConfig.enableAccessPositionIndex,
//              optimiseForReverseIteration = sortedIndexConfig.optimiseForReverseIteration
//            )
//
//          implicit val segmentConfig = SegmentBlockConfig.random.copy(minSize = closedStats.totalValuesSize + closedStats.maxSortedIndexSize, maxCount = stats.keyValues.size)
//
//          MergeStatsSizeCalculator.persistentSizeCalculator.isStatsOrNullSmall(statsOrNull = stats) shouldBe false
//        }
//      }
//
//      "segmentSize is small but maxCount over the limit" in {
//        runThis(100.times, log = true) {
//
//          val stats = MergeStatsCreator.PersistentCreator.create(randomBoolean())
//          stats.addOne(Memory.put(1, 1))
//          stats.addOne(Memory.put(2, 2))
//          stats.addOne(Memory.put(3, 3))
//          stats.addOne(Memory.put(4, 4))
//
//          implicit val sortedIndexConfig: SortedIndexBlockConfig = SortedIndexBlockConfig.random
//          implicit val segmentConfig = SegmentBlockConfig.random.copy(minSize = Int.MaxValue, maxCount = randomIntMax(stats.keyValues.size))
//
//          MergeStatsSizeCalculator.persistentSizeCalculator.isStatsOrNullSmall(statsOrNull = stats) shouldBe false
//        }
//      }
//    }
//
//    "return true" when {
//      "segmentSize and maxCount do not exceed limit" in {
//        runThis(100.times, log = true) {
//
//          val stats = MergeStatsCreator.PersistentCreator.create(randomBoolean())
//          stats.addOne(Memory.put(1, 1))
//          stats.addOne(Memory.put(2, 2))
//          stats.addOne(Memory.put(3, 3))
//          stats.addOne(Memory.put(4, 4))
//
//          implicit val sortedIndexConfig: SortedIndexBlockConfig = SortedIndexBlockConfig.random
//
//          val closedStats =
//            stats.close(
//              hasAccessPositionIndex = sortedIndexConfig.enableAccessPositionIndex,
//              optimiseForReverseIteration = sortedIndexConfig.optimiseForReverseIteration
//            )
//
//          implicit val segmentConfig = SegmentBlockConfig.random.copy(minSize = ((closedStats.totalValuesSize + closedStats.maxSortedIndexSize) * 3) + 1, maxCount = stats.keyValues.size + 1)
//
//          MergeStatsSizeCalculator.persistentSizeCalculator.isStatsOrNullSmall(statsOrNull = stats) shouldBe true
//        }
//      }
//
//      "one key-value is removable" in {
//        runThis(100.times, log = true) {
//
//          val stats = MergeStatsCreator.PersistentCreator.create(true)
//          stats.addOne(Memory.put(1, 1))
//          stats.addOne(Memory.put(2, 2))
//          stats.addOne(Memory.put(3, 3))
//          stats.addOne(Memory.remove(4))
//
//          implicit val sortedIndexConfig: SortedIndexBlockConfig = SortedIndexBlockConfig.random
//          implicit val segmentConfig = SegmentBlockConfig.random.copy(minSize = Int.MaxValue, maxCount = 4)
//
//          MergeStatsSizeCalculator.persistentSizeCalculator.isStatsOrNullSmall(statsOrNull = stats) shouldBe true
//        }
//      }
//    }
//  }
//}
