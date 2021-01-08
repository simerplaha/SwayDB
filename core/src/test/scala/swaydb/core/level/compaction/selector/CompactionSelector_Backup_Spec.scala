/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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

package swaydb.core.level.compaction.selector

import swaydb.core.TestBase

class CompactionSelector_Backup_Spec extends TestBase {

  //  "optimalSegmentsToPushForward" should {
  //    "return empty if there Levels are empty" in {
  //      TestCaseSweeper {
  //        implicit sweeper =>
  //          val nextLevel = TestLevel()
  //          val level = TestLevel()
  //          implicit val reserve = ReserveRange.create[Unit]()
  //
  //          Level.optimalSegmentsToPushForward(
  //            level = level,
  //            nextLevel = nextLevel,
  //            take = 10
  //          ) shouldBe Level.emptySegmentsToPush
  //      }
  //    }
  //
  //    "return all Segments to copy if next Level is empty" in {
  //      TestCaseSweeper {
  //        implicit sweeper =>
  //          val nextLevel = TestLevel()
  //          val level = TestLevel(keyValues = randomizedKeyValues(count = 10000, startId = Some(1)), segmentConfig = SegmentBlock.Config.random(minSegmentSize = 1.kb, mmap = mmapSegments))
  //          //      level.segmentsCount() should be >= 2
  //
  //          implicit val reserve = ReserveRange.create[Unit]()
  //
  //          val (toCopy, toMerge) =
  //            Level.optimalSegmentsToPushForward(
  //              level = level,
  //              nextLevel = nextLevel,
  //              take = 10
  //            )
  //
  //          toMerge shouldBe empty
  //          toCopy.map(_.path) shouldBe level.segments().take(10).map(_.path)
  //      }
  //    }
  //
  //    "return all unreserved Segments to copy if next Level is empty" in {
  //      TestCaseSweeper {
  //        implicit sweeper =>
  //          val nextLevel = TestLevel()
  //          val level = TestLevel(keyValues = randomizedKeyValues(count = 10000, startId = Some(1)), segmentConfig = SegmentBlock.Config.random(minSegmentSize = 1.kb, mmap = mmapSegments))
  //
  //          level.segmentsCount() should be >= 2
  //
  //          implicit val reserve = ReserveRange.create[Unit]()
  //          val firstSegment = level.segments().head
  //
  //          ReserveRange.reserveOrGet(firstSegment.minKey, firstSegment.maxKey.maxKey, firstSegment.maxKey.inclusive, ()) shouldBe empty //reserve first segment
  //
  //          val (toCopy, toMerge) =
  //            Level.optimalSegmentsToPushForward(
  //              level = level,
  //              nextLevel = nextLevel,
  //              take = 10
  //            )
  //
  //          toMerge shouldBe empty
  //          toCopy.map(_.path) shouldBe level.segments().drop(1).take(10).map(_.path)
  //      }
  //    }
  //  }
  //
  //  "optimalSegmentsToCollapse" should {
  //    "return empty if there Levels are empty" in {
  //      TestCaseSweeper {
  //        implicit sweeper =>
  //          val level = TestLevel()
  //
  //          implicit val reserve = ReserveRange.create[Unit]()
  //
  //          Level.optimalSegmentsToCollapse(
  //            level = level,
  //            take = 10
  //          ) shouldBe empty
  //      }
  //    }
  //
  //    "return empty if all segments were reserved" in {
  //      TestCaseSweeper {
  //        implicit sweeper =>
  //          val keyValues = randomizedKeyValues(count = 10000, startId = Some(1))
  //          val level = TestLevel(keyValues = keyValues, segmentConfig = SegmentBlock.Config.random(minSegmentSize = 1.kb, mmap = mmapSegments))
  //
  //          level.segmentsCount() should be >= 2
  //
  //          implicit val reserve = ReserveRange.create[Unit]()
  //
  //          val minKey = keyValues.head.key
  //          val maxKey = Segment.minMaxKey(level.segments()).get
  //          ReserveRange.reserveOrGet(minKey, maxKey._2, maxKey._3, ()) shouldBe empty
  //
  //          Level.optimalSegmentsToCollapse(
  //            level = level,
  //            take = 10
  //          ) shouldBe empty
  //      }
  //    }
  //  }


}
