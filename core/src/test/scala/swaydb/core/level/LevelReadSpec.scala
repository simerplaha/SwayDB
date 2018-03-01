/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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

import org.scalamock.scalatest.MockFactory
import swaydb.core.TestBase
import swaydb.core.data.{KeyValue, Transient}
import swaydb.core.util.Benchmark
import swaydb.core.util.PipeOps._
import swaydb.data.compaction.{LevelMeter, Throttle}
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.order.KeyOrder
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.core.util.FileUtil._

import scala.concurrent.duration._

//@formatter:off
class LevelReadSpec1 extends LevelReadSpec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = true
  override def mmapSegmentsOnRead = true
  override def level0MMAP = true
  override def appendixStorageMMAP = true
}

class LevelReadSpec2 extends LevelReadSpec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = false
  override def mmapSegmentsOnRead = false
  override def level0MMAP = false
  override def appendixStorageMMAP = false
}

class LevelReadSpec3 extends LevelReadSpec {
  override def inMemoryStorage = true
}
//@formatter:on

class LevelReadSpec extends TestBase with MockFactory with Benchmark {

  implicit val ordering: Ordering[Slice[Byte]] = KeyOrder.default
  val keyValuesCount = 100

  "Level.getFromThisLevel" should {
    "return key-value from current level" in {
      //mock nextLevel. It should get any request from upper level when invoking getFromThisLevel
      val nextLevel = mock[LevelRef]
      nextLevel.isTrash _ expects() returning false repeat 2.times

      val level = TestLevel(nextLevel = Some(nextLevel))
      val keyValues = randomIntKeyValues(keyValuesCount, addRandomDeletes = true)
      level.putKeyValues(keyValues).assertGet
      level.putKeyValues(keyValues.lastSlice).assertGet

      assertGetFromThisLevelOnly(keyValues, level)
      //key-values that do not exist in the Level
      val startFrom = keyValues.last.key.read[Int]
      val notExistKeyValues =
        Slice(
          Transient.Put(startFrom + 1, startFrom + 1),
          Transient.Put(startFrom + 2, startFrom + 2),
          Transient.Put(startFrom + 3, startFrom + 3)
        )

      assertGetNoneFromThisLevelOnly(notExistKeyValues, level)
    }
  }

  "Level.get" should {
    "return key-value this level and all child levels" in {
      val keyValues = randomIntKeyValues(keyValuesCount)

      val (keyValues1, keyValues2, keyValues3) =
        keyValues
          .splitAt(keyValuesCount / 3)
          .==> {
            case (split1, split2) =>
              val (two, three) = split2.splitAt(split2.size / 2)
              (split1.updateStats, two.updateStats, three.updateStats)
          }

      val level3 = TestLevel(nextLevel = None)
      level3.putKeyValues(keyValues3).assertGet
      assertGetFromThisLevelOnly(keyValues3, level3)

      val level2 = TestLevel(nextLevel = Some(level3))
      level2.putKeyValues(keyValues2).assertGet
      assertGetFromThisLevelOnly(keyValues2, level2)
      assertGet(keyValues3, level2) //level2 should read key-values from level3
      assertGetNone(keyValues1, level2) //level2 should not know about level1's key-values

      val level1 = TestLevel(nextLevel = Some(level2))
      level1.putKeyValues(keyValues1).assertGet
      assertGetFromThisLevelOnly(keyValues1, level1)
      assertReads(keyValues, level1)
      if (persistent) assertReads(keyValues, level1.reopen)
    }
  }

  "Level.mightContain" should {
    "return true for key-values that exists or else false (bloom filter test on reboot)" in {
      val keyValues = randomIntKeyValues(keyValuesCount)

      def assert(level: Level) = {
        keyValues foreach {
          keyValue =>
            level.mightContain(keyValue.key).assertGet shouldBe true
        }

        level.mightContain("DOES NOT EXISTS").assertGet shouldBe false
      }

      val level = TestLevel()
      level.putKeyValues(keyValues).assertGet

      assert(level)
      if (persistent) assert(level.reopen)
    }
  }

  "Level.lowerInThisLevel" should {
    "get the lowest key from the level" in {
      val nextLevel = mock[LevelRef]
      nextLevel.isTrash _ expects() returning false repeat 2.times

      val keyValues = randomIntKeyValues(keyValuesCount)

      val level = TestLevel(segmentSize = 100.bytes, nextLevel = Some(nextLevel))
      level.putKeyValues(keyValues).assertGet
      level.putKeyValues(keyValues.headSlice).assertGet

      assertLowerFromThisLevel(keyValues, level)
    }
  }

  "Level.lower, Level.higher, Level.head, Level.last" should {
    "get the lowest, highest, first and last from the level and all it's lower level" in {
      val keyValues = randomIntKeyValues(keyValuesCount)

      val (keyValues1, keyValues2, keyValues3) =
        keyValues
          .splitAt(keyValuesCount / 3)
          .==> {
            case (split1, split2) =>
              val (two, three) = split2.splitAt(split2.size / 2)
              (split1.updateStats, two.updateStats, three.updateStats)
          }

      val level3 = TestLevel(nextLevel = None, segmentSize = 1.byte, throttle = (_) => Throttle(Duration.Zero, 0))
      level3.putKeyValues(keyValues3).assertGet
      level3.putKeyValues(keyValues3.headSlice).assertGet

      val level2 = TestLevel(nextLevel = Some(level3), segmentSize = 1.byte, throttle = (_) => Throttle(Duration.Zero, 0))
      level2.putKeyValues(keyValues2).assertGet
      level2.putKeyValues(keyValues2.headSlice).assertGet

      val level1 = TestLevel(nextLevel = Some(level2), segmentSize = 1.byte, throttle = (_) => Throttle(Duration.Zero, 0))
      level1.putKeyValues(keyValues1).assertGet
      level1.putKeyValues(keyValues1.headSlice).assertGet

      assertLower(keyValues, level1)
      assertHigher(keyValues, level1)
      level1.head.assertGet shouldBe keyValues.head
      level1.last.assertGet shouldBe keyValues.last

      if (persistent) {
        val reopen1 = level1.reopen
        assertLower(keyValues, reopen1)

        val reopen2 = reopen1.reopen
        assertHigher(keyValues, reopen2)

        val reopen3 = reopen2.reopen
        reopen3.head.assertGet shouldBe keyValues.head
        reopen3.last.assertGet shouldBe keyValues.last
      }
    }
  }

  "Level.takeSmallSegments" should {
    "filter smaller segments from a Level" in {
      //disable throttling so small segment compaction does not occur
      val level = TestLevel(segmentSize = 1.kb, nextLevel = None, throttle = (_) => Throttle(Duration.Zero, 0))

      val keyValues = randomIntKeyValues(1000)
      level.putKeyValues(keyValues).assertGet
      //do another put so split occurs.
      level.putKeyValues(keyValues.headSlice).assertGet
      level.segmentsCount() > 1 shouldBe true //ensure there are Segments in this Level

      if (persistent) {
        val reopen = level.reopen(segmentSize = 10.mb)

        reopen.takeSmallSegments(10000) should not be empty
        //iterate again on the same Iterable.
        // This test is to ensure that returned List is not a java Iterable which is only iterable once.
        reopen.takeSmallSegments(10000) should not be empty

        reopen.reopen(segmentSize = 10.mb).takeLargeSegments(1) shouldBe empty
      }
    }
  }

  "Level.pickSegmentsToPush" should {
    "return segments in sequence when there is no lower level (These picks are for collapsing segments within the level)" in {
      val segments = (1 to 10) map (index => TestSegment(Slice(Transient.Put(index, index))).assertGet)

      val level = TestLevel(segmentSize = 1.byte)
      level.put(segments).assertGet
      level.putKeyValues(Slice(Transient.Put(1, 1))).assertGet

      level.pickSegmentsToPush(0) shouldBe empty
      level.pickSegmentsToPush(5) shouldHaveSameKeyValuesAs segments.take(5)
      level.pickSegmentsToPush(10) shouldHaveSameKeyValuesAs segments
      level.pickSegmentsToPush(20) shouldHaveSameKeyValuesAs segments
    }

    "return segments in sequence when there are busy segments in lower level" in {
      val segments = (1 to 10) map (index => TestSegment(Slice(Transient.Put(index, index))).assertGet)
      val nextLevel = mock[LevelRef]
      nextLevel.isTrash _ expects() returning false repeat 2.times

      val level = TestLevel(segmentSize = 1.byte, nextLevel = Some(nextLevel), throttle = (_) => Throttle(Duration.Zero, 0))
      level.put(segments).assertGet
      level.putKeyValues(Slice(Transient.Put(1, 1))).assertGet

      level.pickSegmentsToPush(0) shouldBe empty

      nextLevel.getBusySegments _ expects() returning List.empty repeat 4.times
      level.pickSegmentsToPush(1) shouldHaveSameKeyValuesAs segments.take(1)
      level.pickSegmentsToPush(5) shouldHaveSameKeyValuesAs segments.take(5)
      level.pickSegmentsToPush(10) shouldHaveSameKeyValuesAs segments
      level.pickSegmentsToPush(20) shouldHaveSameKeyValuesAs segments

      nextLevel.getBusySegments _ expects() returning segments.take(5).toList repeat 6.times
      level.pickSegmentsToPush(5) shouldHaveSameKeyValuesAs segments.drop(5)
      level.pickSegmentsToPush(1) shouldHaveSameKeyValuesAs segments.drop(5).take(1)
      level.pickSegmentsToPush(3) shouldHaveSameKeyValuesAs segments.drop(5).take(3)
      level.pickSegmentsToPush(8) shouldHaveSameKeyValuesAs segments.drop(5)
      level.pickSegmentsToPush(10) shouldHaveSameKeyValuesAs segments.drop(5)
      level.pickSegmentsToPush(20) shouldHaveSameKeyValuesAs segments.drop(5)

      nextLevel.getBusySegments _ expects() returning segments.drop(5).toList repeat 6.times
      level.pickSegmentsToPush(5) shouldHaveSameKeyValuesAs segments.take(5)
      level.pickSegmentsToPush(1) shouldHaveSameKeyValuesAs segments.take(1)
      level.pickSegmentsToPush(3) shouldHaveSameKeyValuesAs segments.take(3)
      level.pickSegmentsToPush(8) shouldHaveSameKeyValuesAs segments.take(5)
      level.pickSegmentsToPush(10) shouldHaveSameKeyValuesAs segments.take(5)
      level.pickSegmentsToPush(20) shouldHaveSameKeyValuesAs segments.take(5)
    }
  }

  "Level.meter" should {
    "return Level stats" in {
      val level = TestLevel()

      val segment = TestSegment().assertGet
      level.put(Seq(segment)).assertGet

      level.meter shouldBe LevelMeter(1, segment.segmentSize)
    }
  }

  "Level.meterFor" should {
    "forward request to the right level" in {
      val level2 = TestLevel()
      val level1 = TestLevel(nextLevel = Some(level2))

      val segment = TestSegment().assertGet
      level2.put(Seq(segment)).assertGet

      level1.meter shouldBe LevelMeter(0, 0)
      level1.meterFor(level1.paths.headPath.folderId.toInt) should contain(LevelMeter(0, 0))

      level2.meter shouldBe LevelMeter(1, segment.segmentSize)
      level1.meterFor(level2.paths.headPath.folderId.toInt) should contain(LevelMeter(1, segment.segmentSize))
    }

    "return None is Level does not exist" in {
      val level2 = TestLevel()
      val level1 = TestLevel(nextLevel = Some(level2))

      val segment = TestSegment().assertGet
      level2.put(Seq(segment)).assertGet

      level1.meterFor(3) shouldBe empty
    }
  }

}
