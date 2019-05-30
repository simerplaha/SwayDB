package swaydb.core.level.compaction

import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.queue.{FileLimiter, KeyValueLimiter}
import swaydb.core.{TestBase, TestLimitQueues}
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

sealed trait CompactionSpec extends TestBase {

  val keyValueCount = 1000

  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long

  implicit val maxSegmentsOpenCacheImplicitLimiter: FileLimiter = TestLimitQueues.fileOpenLimiter
  implicit val keyValuesLimitImplicitLimiter: KeyValueLimiter = TestLimitQueues.keyValueLimiter

  "copyForwardOnAllLevels" should {
    "not copy" when {
      "it's the last Level and is empty" in {
        Compaction.copyForwardOnAllLevels(TestLevel()) shouldBe 0
      }

      "it's the last Level and is non empty" in {
        val keyValues = randomizedKeyValues(keyValueCount).toMemory
        val level = TestLevel(keyValues = keyValues)
        level.isEmpty shouldBe false
        Compaction.copyForwardOnAllLevels(level) shouldBe 0
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
        Compaction.copyForwardOnAllLevels(level1) shouldBe expectedCopiedSegments
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

        Compaction.copyForwardOnAllLevels(level1)

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
}
