///*
// * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
// *
// * This file is a part of SwayDB.
// *
// * SwayDB is free software: you can redistribute it and/or modify
// * it under the terms of the GNU Affero General Public License as
// * published by the Free Software Foundation, either version 3 of the
// * License, or (at your option) any later version.
// *
// * SwayDB is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// * GNU Affero General Public License for more details.
// *
// * You should have received a copy of the GNU Affero General Public License
// * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
// *
// * Additional permission under the GNU Affero GPL version 3 section 7:
// * If you modify this Program or any covered work, only by linking or combining
// * it with separate works, the licensors of this Program grant you additional
// * permission to convey the resulting work.
// */
//
//package swaydb.core.segment.defrag
//
//import org.scalamock.scalatest.MockFactory
//import org.scalatest.EitherValues
//import swaydb.core.TestData._
//import swaydb.core.data.Memory
//import swaydb.core.merge.MergeStatsCreator
//import swaydb.core.segment.block.segment.SegmentBlock
//import swaydb.core.segment.block.sortedindex.SortedIndexBlock
//import swaydb.core.{TestBase, TestExecutionContext, TestTimer}
//import swaydb.testkit.RunThis._
//import swaydb.serializers.Default._
//import swaydb.serializers._
//
//class DefragCommonSpec extends TestBase with MockFactory with EitherValues {
//
//  implicit val ec = TestExecutionContext.executionContext
//  implicit val timer = TestTimer.Empty
//
//  "isStatsSmall" should {
//    "return false" when {
//
//      "stats is null" in {
//        implicit val sortedIndexConfig: SortedIndexBlock.Config = SortedIndexBlock.Config.random
//        implicit val segmentConfig = SegmentBlock.Config.random
//
//        DefragCommon.isStatsOrNullSmall(statsOrNull = null) shouldBe false
//      }
//
//      "segmentSize and maxCount exceed limit" in {
//        runThis(100.times, log = true) {
//
//          val stats = MergeStatsCreator.PersistentCreator.create(randomBoolean())
//          stats.add(Memory.put(1, 1))
//          stats.add(Memory.put(2, 2))
//          stats.add(Memory.put(3, 3))
//          stats.add(Memory.put(4, 4))
//
//          implicit val sortedIndexConfig: SortedIndexBlock.Config = SortedIndexBlock.Config.random
//
//          val closedStats =
//            stats.close(
//              hasAccessPositionIndex = sortedIndexConfig.enableAccessPositionIndex,
//              optimiseForReverseIteration = sortedIndexConfig.optimiseForReverseIteration
//            )
//
//          implicit val segmentConfig = SegmentBlock.Config.random.copy(minSize = closedStats.totalValuesSize + closedStats.maxSortedIndexSize, maxCount = stats.keyValues.size)
//
//          DefragCommon.isStatsOrNullSmall(statsOrNull = stats) shouldBe false
//        }
//      }
//
//      "segmentSize is small but maxCount over the limit" in {
//        runThis(100.times, log = true) {
//
//          val stats = MergeStatsCreator.PersistentCreator.create(randomBoolean())
//          stats.add(Memory.put(1, 1))
//          stats.add(Memory.put(2, 2))
//          stats.add(Memory.put(3, 3))
//          stats.add(Memory.put(4, 4))
//
//          implicit val sortedIndexConfig: SortedIndexBlock.Config = SortedIndexBlock.Config.random
//          implicit val segmentConfig = SegmentBlock.Config.random.copy(minSize = Int.MaxValue, maxCount = randomIntMax(stats.keyValues.size))
//
//          DefragCommon.isStatsOrNullSmall(statsOrNull = stats) shouldBe false
//        }
//      }
//    }
//
//    "return true" when {
//      "segmentSize and maxCount do not exceed limit" in {
//        runThis(100.times, log = true) {
//
//          val stats = MergeStatsCreator.PersistentCreator.create(randomBoolean())
//          stats.add(Memory.put(1, 1))
//          stats.add(Memory.put(2, 2))
//          stats.add(Memory.put(3, 3))
//          stats.add(Memory.put(4, 4))
//
//          implicit val sortedIndexConfig: SortedIndexBlock.Config = SortedIndexBlock.Config.random
//
//          val closedStats =
//            stats.close(
//              hasAccessPositionIndex = sortedIndexConfig.enableAccessPositionIndex,
//              optimiseForReverseIteration = sortedIndexConfig.optimiseForReverseIteration
//            )
//
//          implicit val segmentConfig = SegmentBlock.Config.random.copy(minSize = ((closedStats.totalValuesSize + closedStats.maxSortedIndexSize) * 3) + 1, maxCount = stats.keyValues.size + 1)
//
//          DefragCommon.isStatsOrNullSmall(statsOrNull = stats) shouldBe true
//
//        }
//      }
//
//      "one key-value is removable" in {
//        runThis(100.times, log = true) {
//
//          val stats = MergeStatsCreator.PersistentCreator.create(true)
//          stats.add(Memory.put(1, 1))
//          stats.add(Memory.put(2, 2))
//          stats.add(Memory.put(3, 3))
//          stats.add(Memory.remove(4))
//
//          implicit val sortedIndexConfig: SortedIndexBlock.Config = SortedIndexBlock.Config.random
//          implicit val segmentConfig = SegmentBlock.Config.random.copy(minSize = Int.MaxValue, maxCount = 4)
//
//          DefragCommon.isStatsOrNullSmall(statsOrNull = stats) shouldBe true
//        }
//      }
//    }
//  }
//}
