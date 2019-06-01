package swaydb.core.level.compaction

import org.scalamock.scalatest.MockFactory
import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.level.Level
import swaydb.core.queue.{FileLimiter, KeyValueLimiter}
import swaydb.core.segment.Segment
import swaydb.core.{TestBase, TestLimitQueues}
import swaydb.data.IO
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._

class CompactionSpec0 extends CompactionSpec

class CompactionSpec1 extends CompactionSpec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = true
  override def mmapSegmentsOnRead = true
  override def level0MMAP = true
  override def appendixStorageMMAP = true
}

class CompactionSpec2 extends CompactionSpec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = false
  override def mmapSegmentsOnRead = false
  override def level0MMAP = false
  override def appendixStorageMMAP = false
}

class CompactionSpec3 extends CompactionSpec {
  override def inMemoryStorage = true
}

sealed trait CompactionSpec extends TestBase with MockFactory {

  val keyValueCount = 1000

  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long

  implicit val maxSegmentsOpenCacheImplicitLimiter: FileLimiter = TestLimitQueues.fileOpenLimiter
  implicit val keyValuesLimitImplicitLimiter: KeyValueLimiter = TestLimitQueues.keyValueLimiter

  "copyForwardForEach" should {
    "not copy" when {
      "it's the last Level and is empty" in {
        Compaction.copyForwardForEach(Slice(TestLevel())) shouldBe 0
      }

      "it's the last Level and is non empty" in {
        val keyValues = randomPutKeyValues(keyValueCount).toMemory
        val level = TestLevel(keyValues = keyValues)
        level.isEmpty shouldBe false
        Compaction.copyForwardForEach(level.reverseLevels.toSlice) shouldBe 0
        assertGet(keyValues, level.reopen)
      }
    }

    "copy all Segments to last level" when {
      "no Segments overlap" in {
        val allKeyValues = randomPutKeyValues(keyValueCount, startId = Some(1)).toMemory
        val keyValues = allKeyValues.groupedSlice(5)

        val level5 = TestLevel(keyValues = keyValues(4), segmentSize = 2.kb)
        val level4 = TestLevel(nextLevel = Some(level5), keyValues = keyValues(3), segmentSize = 2.kb)
        val level3 = TestLevel(nextLevel = Some(level4), keyValues = keyValues(2), segmentSize = 2.kb)
        val level2 = TestLevel(nextLevel = Some(level3), keyValues = keyValues(1), segmentSize = 2.kb)
        val level1 = TestLevel(nextLevel = Some(level2), keyValues = keyValues(0), segmentSize = 2.kb)

        level1.foreachLevel(_.segmentsCount() should be > 1)

        val expectedCopiedSegments = level1.foldLeftLevels(0)(_ + _.segmentsCount()) - level5.segmentsCount()
        Compaction.copyForwardForEach(level1.reverseLevels.toSlice) shouldBe expectedCopiedSegments
        //all top levels shouldBe empty
        level1.mapLevels(level => level).dropRight(1).foreach(_.isEmpty shouldBe true)

        assertReads(allKeyValues, level1)
        assertReads(allKeyValues, level2)
        assertReads(allKeyValues, level3)
        assertReads(allKeyValues, level4)
        assertReads(allKeyValues, level5)

        assertGet(allKeyValues, level1.reopen)
      }
    }

    "copy Segments to last level" when {
      "some Segments overlap" in {
        val allKeyValues = randomPutKeyValues(keyValueCount, addRandomPutDeadlines = false, startId = Some(1)).toMemory

        val keyValues = allKeyValues.groupedSlice(5)

        val level5 = TestLevel(keyValues = Slice(keyValues(3).last) ++ keyValues(4), segmentSize = 2.kb)
        val level4 = TestLevel(nextLevel = Some(level5), keyValues = Slice(keyValues(2).last) ++ keyValues(3), segmentSize = 2.kb)
        val level3 = TestLevel(nextLevel = Some(level4), keyValues = Slice(keyValues(1).last) ++ keyValues(2), segmentSize = 2.kb)
        val level2 = TestLevel(nextLevel = Some(level3), keyValues = Slice(keyValues(0).last) ++ keyValues(1), segmentSize = 2.kb)
        val level1 = TestLevel(nextLevel = Some(level2), keyValues = keyValues(0), segmentSize = 2.kb)

        Compaction.copyForwardForEach(level1.reverseLevels.toSlice)

        //top levels are level, second last level get all overlapping Segments, last Level gets the rest.
        level1.isEmpty shouldBe true
        level2.isEmpty shouldBe true
        level3.isEmpty shouldBe true
        level4.isEmpty shouldBe false
        level5.isEmpty shouldBe false

        assertReads(allKeyValues, level1)
        assertGet(allKeyValues, level1.reopen)
      }
    }
  }

  "putForward" should {
    "return zero" when {
      "input Segments are empty" in {
        //levels are never invoked
        val thisLevel = mock[Level]("thisLevel")
        val nextLevel = mock[Level]("nextLevel")

        Compaction.putForward(Iterable.empty, thisLevel, nextLevel) shouldBe IO.zero
      }
    }

    "remove Segments" when {

      "Segments from upper Level are merged into lower level" in {
        val thisLevel = mock[Level]("thisLevel")
        val nextLevel = mock[Level]("nextLevel")

        val keyValues = randomPutKeyValues(keyValueCount).groupedSlice(2)
        val segments = Seq(TestSegment(keyValues(0).toTransient).get, TestSegment(keyValues(1).toTransient).get)

        //next level should get a put for all the input Segments
        (nextLevel.put(_: Iterable[Segment])) expects * onCall {
          putSegments: Iterable[Segment] =>
            putSegments shouldHaveSameIds segments
            IO.unit
        }

        //segments get removed
        (thisLevel.removeSegments(_: Iterable[Segment])) expects * onCall {
          putSegments: Iterable[Segment] =>
            putSegments shouldHaveSameIds segments
            IO.Success(segments.size)
        }

        Compaction.putForward(segments, thisLevel, nextLevel).get shouldBe segments.size
      }
    }

    "return success" when {

      "it fails to remove Segments" in {
        val thisLevel = mock[Level]("thisLevel")
        val nextLevel = mock[Level]("nextLevel")

        val keyValues = randomPutKeyValues(keyValueCount).groupedSlice(2)
        val segments = Seq(TestSegment(keyValues(0).toTransient).get, TestSegment(keyValues(1).toTransient).get)

        //next level should get a put for all the input Segments
        (nextLevel.put(_: Iterable[Segment])) expects * onCall {
          putSegments: Iterable[Segment] =>
            putSegments shouldHaveSameIds segments
            IO.unit
        }

        //segments get removed
        (thisLevel.removeSegments(_: Iterable[Segment])) expects * onCall {
          putSegments: Iterable[Segment] =>
            putSegments shouldHaveSameIds segments
            IO.Failure(new Exception("Failed!"))
        }

        Compaction.putForward(segments, thisLevel, nextLevel).get shouldBe segments.size
      }
    }
  }

  "runLastLevelCompaction" should {
    "not run compaction" when {
      "level is not the last Level" in {
        val level = mock[Level]("level")
        level.hasNextLevel _ expects() returns false

        Compaction.runLastLevelCompaction(level, true, 100, 0) shouldBe IO.zero
      }

      "remaining compactions are 0" in {
        val level = mock[Level]("level")
        level.hasNextLevel _ expects() returns true

        Compaction.runLastLevelCompaction(level, true, remainingCompactions = 0, 10) shouldBe IO.Success(10)
      }
    }
  }
}
