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

package swaydb.core.segment

import java.nio.file._

import swaydb.core.data.Transient.Delete
import swaydb.core.data.{KeyValue, PersistentReadOnly}
import swaydb.core.io.file.{DBFile, IO}
import swaydb.core.level.PathsDistributor
import swaydb.core.segment.SegmentException.CannotCopyInMemoryFiles
import swaydb.core.util.FileUtil._
import swaydb.core.util._
import swaydb.core.{LimitQueues, TestBase}
import swaydb.data.config.Dir
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.order.KeyOrder
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.collection.JavaConverters._
import scala.concurrent.duration._

//@formatter:off
class SegmentWriteSpec1 extends SegmentWriteSpec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = true
  override def mmapSegmentsOnRead = true
  override def level0MMAP = true
  override def appendixStorageMMAP = true
}

class SegmentWriteSpec2 extends SegmentWriteSpec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = false
  override def mmapSegmentsOnRead = false
  override def level0MMAP = false
  override def appendixStorageMMAP = false
}

class SegmentWriteSpec3 extends SegmentWriteSpec {
  override def inMemoryStorage = true
}
//@formatter:on

class SegmentWriteSpec extends TestBase with Benchmark {

  implicit val ordering = KeyOrder.default
  val keyValuesCount = 10

  //  override def deleteFiles = false

  implicit val fileOpenLimiterImplicit: DBFile => Unit = fileOpenLimiter
  implicit val keyValueLimiterImplicit: (PersistentReadOnly, Segment) => Unit = keyValueLimiter

  "Segment" should {

    "create a Segment" in {
      val keyValues = randomIntKeyValues(keyValuesCount, addRandomDeletes = true)
      val segment = TestSegment(keyValues).assertGet

      assertReads(keyValues, segment)
      segment.minKey shouldBe keyValues.head.key
      segment.maxKey shouldBe keyValues.last.key
      segment.minKey.underlyingArraySize shouldBe 4
      segment.maxKey.underlyingArraySize shouldBe 4

      keyValues.foreach {
        keyValue =>
          segment.getBloomFilter.assertGet.mightContain(keyValue.key) shouldBe true
      }
    }

    "create a Segment and populate cache" in {
      val keyValues = Slice(KeyValue("a", 1), KeyValue("b", 2), KeyValue("c", 3), KeyValue("d", 4), KeyValue("e", 5), KeyValue("f", 6)).updateStats
      val segment = TestSegment(keyValues, cacheKeysOnCreate = true).assertGet

      keyValues foreach {
        keyValue =>
          eventual(segment.isInCache(keyValue.key) shouldBe true)
          segment.cache.asScala.foreach(_._1.underlyingArraySize shouldBe 1)
          val result = segment.get(keyValue.key).assertGet
          result shouldBe keyValue
          result.getOrFetchValue.assertGet.underlyingArraySize shouldBe 4
      }

      assertBloom(keyValues, segment.getBloomFilter.assertGet)
    }

    "not overwrite a Segment if it already exists" in {
      if (memory) {
        // memory Segments do not check for overwrite. No tests required
      } else {
        val keyValues = randomIntKeyValues(keyValuesCount, addRandomDeletes = true)
        val failedKeyValues = randomIntKeyValues(keyValuesCount, addRandomDeletes = true)
        val segment = TestSegment(keyValues).assertGet

        val failed = TestSegment(failedKeyValues, path = segment.path)
        failed.failed.get shouldBe a[FileAlreadyExistsException]

        //data remained unchanged
        assertReads(keyValues, segment)
        failedKeyValues foreach {
          keyValue =>
            segment.get(keyValue.key).assertGetOpt.isEmpty shouldBe true
        }
        assertBloom(keyValues, segment.getBloomFilter.assertGet)
      }
    }

    "initialise a segment that already exists" in {
      if (memory) {
        //memory Segments cannot re-initialise Segments after shutdown.
      } else {
        val keyValues = randomIntKeyValues(keyValuesCount, addRandomDeletes = true)
        val segmentFile = testSegmentFile

        TestSegment(keyValues, path = segmentFile).assertGet
        val readSegment = Segment(
          path = segmentFile,
          mmapReads = levelStorage.mmapSegmentsOnRead,
          mmapWrites = levelStorage.mmapSegmentsOnWrite,
          cacheKeysOnCreate = false,
          minKey = keyValues.head.key,
          maxKey = keyValues.last.key,
          segmentSize = keyValues.last.stats.segmentSize,
          removeDeletes = false
        ).assertGet

        //ensure that Segments opened for reads and lazily loaded.
        readSegment.isOpen shouldBe false
        readSegment.isFileDefined shouldBe false
        assertReads(keyValues, readSegment)
        readSegment.isOpen shouldBe true
        readSegment.isFileDefined shouldBe true

        assertBloom(keyValues, readSegment.getBloomFilter.assertGet)
      }
    }

    "fail initialisation if the segment does not exist" in {
      if (memory) {
        //memory Segments do not get re-initialised
      } else {
        Segment(
          path = testSegmentFile,
          mmapReads = levelStorage.mmapSegmentsOnRead,
          mmapWrites = levelStorage.mmapSegmentsOnWrite,
          cacheKeysOnCreate = false,
          minKey = Slice.create[Byte](0),
          maxKey = Slice.create[Byte](0),
          segmentSize = 0,
          removeDeletes = false
        ).failed.assertGet shouldBe a[NoSuchFileException]
      }
    }
  }

  "Segment.deleteSegments" should {
    "delete multiple segments" in {
      val segment1 = TestSegment(randomIntKeyValues(keyValuesCount, addRandomDeletes = true)).assertGet
      val segment2 = TestSegment(randomIntKeyStringValues(keyValuesCount, addRandomDeletes = true)).assertGet
      val segment3 = TestSegment(randomIntKeyStringValues(keyValuesCount, addRandomDeletes = true)).assertGet

      val deleted = Segment.deleteSegments(Seq(segment1, segment2, segment3))
      deleted.assertGet shouldBe 3


      //files should be closed
      if (persistent) {
        segment1.isOpen shouldBe false
        segment2.isOpen shouldBe false
        segment3.isOpen shouldBe false
        segment1.isFileDefined shouldBe false
        segment2.isFileDefined shouldBe false
        segment3.isFileDefined shouldBe false
      }
      segment1.isCacheEmpty shouldBe true
      segment2.isCacheEmpty shouldBe true
      segment3.isCacheEmpty shouldBe true
    }
  }

  "Segment" should {
    "open a closed file on read" in {
      val keyValues = randomIntKeyValues(keyValuesCount)
      val segment = TestSegment(keyValues).assertGet

      def close = {
        segment.close.assertGet
        if (levelStorage.persistent) {
          segment.isFileDefined shouldBe false
          segment.isOpen shouldBe false
        }
      }

      def open(keyValue: KeyValue) = {
        segment.get(keyValue.key).assertGet shouldBe keyValue
        segment.isFileDefined shouldBe true
        segment.isOpen shouldBe true
      }

      close
      open(keyValues.head)

      close
      open(keyValues.last)

    }

    "fail get and put operations on a file that does not exists" in {
      if (persistent) {
        val keyValues = randomIntKeyValues(keyValuesCount, addRandomDeletes = true)
        val segment = TestSegment(keyValues).assertGet

        segment.delete.assertGet
        segment.isOpen shouldBe false
        segment.isFileDefined shouldBe false

        segment.existsOnDisk shouldBe false
        segment.get(keyValues.head.key).failed.get shouldBe a[NoSuchFileException]
        segment.put(keyValues, 1.mb, 0.1).failed.get shouldBe a[NoSuchFileException]
        segment.isOpen shouldBe false
        segment.isFileDefined shouldBe false
      }
    }
  }

  "reopen closed channel for read when closed by LimitQueue" in {
    if (memory) {
      //memory Segments do not get closed via
    } else {
      implicit val segmentOpenLimit = LimitQueues.segmentOpenLimiter(1, 100.millisecond)
      val keyValues = randomIntKeyValues(keyValuesCount)
      val segment1 = TestSegment(keyValues)(ordering, keyValueLimiterImplicit, segmentOpenLimit).assertGet
      //if its not mmap'd then read the file so that it gets opened for this test.
      if (!levelStorage.mmapSegmentsOnWrite)
        segment1.getKeyValueCount().assertGet shouldBe keyValues.size

      segment1.isOpen shouldBe true

      //create another segment should close segment 1
      val segment2 = TestSegment(keyValues)(ordering, keyValueLimiterImplicit, segmentOpenLimit).assertGet
      //if its not mmap'd then read the file so that it gets opened for this test.
      if (!levelStorage.mmapSegmentsOnWrite)
        segment2.getKeyValueCount().assertGet shouldBe keyValues.size

      eventual(5.seconds) {
        //segment one is closed
        segment1.isOpen shouldBe false
      }
      //read one key value from Segment1 so that it's reopened and added to the cache. This will also remove Segment 2 from cache
      (segment1 get keyValues.head.key).assertGet shouldBe keyValues.head
      segment1.isOpen shouldBe true

      eventual(20.seconds) {
        //segment2 is closed
        segment2.isOpen shouldBe false
      }
    }
  }

  "Segment.delete" should {
    "clear cache, close the channel and delete the file" in {
      val keyValues = randomIntKeyStringValues(keyValuesCount, addRandomDeletes = true)
      val segment = TestSegment(keyValues).get
      assertReads(keyValues, segment) //populate the cache

      segment.cacheSize shouldBe keyValues.size

      segment.delete.isSuccess shouldBe true
      eventually {
        segment.cacheSize shouldBe 0
      }
      if (persistent) segment.isOpen shouldBe false
      segment.existsOnDisk shouldBe false
    }
  }

  "Segment.copy" should {
    "copy the segment to a target path without deleting the original" in {
      if (memory) {
        val segment = TestSegment(randomIntKeyValues()).assertGet
        segment.copyTo(randomFilePath).failed.assertGet shouldBe CannotCopyInMemoryFiles(segment.path)
      } else {
        val keyValues = randomIntKeyStringValues(keyValuesCount, addRandomDeletes = true)

        val segment = TestSegment(keyValues).get
        val targetSegmentId = nextId
        val targetDir = createRandomIntDirectory
        val targetPath = targetDir.resolve(targetSegmentId + s".${Extension.Seg}")

        segment.copyTo(targetPath).assertGet

        segment.existsOnDisk shouldBe true
        IO.exists(targetDir) shouldBe true

        //load the copied segment
        val copiedSegment = Segment(
          path = segment.path,
          mmapReads = levelStorage.mmapSegmentsOnRead,
          mmapWrites = levelStorage.mmapSegmentsOnWrite,
          cacheKeysOnCreate = false,
          minKey = keyValues.head.key,
          maxKey = keyValues.last.key,
          segmentSize = keyValues.last.stats.segmentSize,
          removeDeletes = false
        ).assertGet

        copiedSegment.getAll(0.1).assertGet shouldBe keyValues

        //original segment should still exist
        segment.getAll(0.1).assertGet shouldBe keyValues
      }
    }
  }

  "Segment.put" should {
    "return None for empty values" in {
      val segment = TestSegment(Slice(KeyValue(1, Slice.create[Byte](0)))).assertGet
      segment.get(1).assertGet.getOrFetchValue.assertGetOpt shouldBe None
    }

    "reopen closed channel" in {
      val keyValue1 = Slice(KeyValue(5, Slice(randomBytes(100))))

      val segment = TestSegment(keyValue1).assertGet
      segment.close.assertGet
      if (persistent) segment.isOpen shouldBe false

      val keyValue2 = Slice(KeyValue(2, Slice(randomBytes(100))), KeyValue(3, Slice(randomBytes(100)))).updateStats
      segment.put(keyValue2, 1.mb, 0.1).assertGet
      if (persistent) segment.isOpen shouldBe true
    }

    "return a new segment with merged key values" in {
      val keyValues = Slice(KeyValue(1, 1))
      val segment = TestSegment(keyValues).assertGet

      val newKeyValues = Slice(KeyValue(2, 2))
      val newSegments = segment.put(newKeyValues, 4.mb, 0.1).assertGet
      newSegments should have size 1

      val allReadKeyValues = Segment.getAllKeyValues(0.1, newSegments).assertGet

      val expectedKeyValues = SegmentMerge.merge(newKeyValues, keyValues, 1.mb, false, forInMemory = memory, 0.1)
      expectedKeyValues should have size 1

      allReadKeyValues shouldBe expectedKeyValues.head
    }

    "return multiple new segments with merged key values" in {
      val keyValues = randomIntKeyStringValues(10000)
      val segment = TestSegment(keyValues).get

      val newKeyValues = randomIntKeyValues(10000)
      val newSegments = segment.put(newKeyValues, 100.kb, 0.1).assertGet
      newSegments.size should be > 1

      val allReadKeyValues = Segment.getAllKeyValues(0.1, newSegments).assertGet

      //give merge a very large size so that there are no splits (test convenience)
      val expectedKeyValues = SegmentMerge.merge(newKeyValues, keyValues, 10.mb, false, forInMemory = memory, 0.1)
      expectedKeyValues should have size 1

      //allReadKeyValues are read from multiple Segments so valueOffsets will be invalid so stats will be invalid
      allReadKeyValues shouldBe(expectedKeyValues.head, ignoreStats = true)
    }

    "fail put and delete partially written batch Segments if there was a failure in creating one of them" in {
      if (memory) {
        // not for in-memory Segments
      } else {

        val keyValues = randomIntKeyStringValues(keyValuesCount)
        val segment = TestSegment(keyValues).get
        val newKeyValues = randomIntKeyValues(10000)

        val tenthSegmentId = {
          val segmentId = (segment.path.fileId.get._1 + 10).toSegmentFileId
          segment.path.getParent.resolve(segmentId)
        }

        //create a segment with the next id in sequence which should fail put with FileAlreadyExistsException
        val segmentToFailPut = TestSegment(path = tenthSegmentId).assertGet

        val failed = segment.put(newKeyValues, 1.kb, 0.1)
        failed.failed.get shouldBe a[FileAlreadyExistsException]

        //the folder should contain only the original segment and the segmentToFailPut
        segment.path.getParent.files(Extension.Seg) should contain only(segment.path, segmentToFailPut.path)
      }
    }

    "return new segment with deleted KeyValues if all keys were deleted and removeDeletes is false" in {
      val keyValues = randomIntKeyValues(count = keyValuesCount)
      val segment = TestSegment(keyValues, removeDeletes = false).assertGet

      keyValues foreach {
        keyValue =>
          segment.get(keyValue.key).assertGet shouldBe keyValue
      }

      val deleteKeyValues = Slice.create[Delete](keyValues.size)
      keyValues.foreach(keyValue => deleteKeyValues add Delete(keyValue.key, 0.1, deleteKeyValues.lastOption))

      val deletedSegment = segment.put(deleteKeyValues, 4.mb, 0.1).assertGet
      deletedSegment should have size 1

      val newDeletedSegment = deletedSegment.head
      newDeletedSegment.getKeyValueCount().assertGet shouldBe keyValues.size
      //segment size calculation should be correct
      if (persistent)
        newDeletedSegment.segmentSize shouldBe deleteKeyValues.last.stats.segmentSize
      else
        newDeletedSegment.segmentSize shouldBe (deleteKeyValues.last.stats.segmentUncompressedKeysSize + deleteKeyValues.last.stats.segmentValuesSize)

      deleteKeyValues foreach {
        keyValue =>
          newDeletedSegment.get(keyValue.key).assertGet shouldBe keyValue
      }
    }

    "return new segment with update KeyValues if all keys values were updated to None" in {
      val keyValues = randomIntKeyStringValues(count = 100)
      val segment = TestSegment(keyValues, removeDeletes = true).assertGet

      val updatedKeyValues = Slice.create[KeyValue](keyValues.size)
      keyValues.foreach(keyValue => updatedKeyValues add KeyValue(keyValue.key, 0.1, updatedKeyValues.lastOption))

      val updatedSegments = segment.put(updatedKeyValues, 4.mb, 0.1).assertGet
      updatedSegments should have size 1

      val newUpdatedSegment = updatedSegments.head
      newUpdatedSegment.getKeyValueCount().assertGet shouldBe keyValues.size
      //segment size calculation should be correct
      if (persistent)
        newUpdatedSegment.segmentSize shouldBe updatedKeyValues.last.stats.segmentSize
      else
        newUpdatedSegment.segmentSize shouldBe (updatedKeyValues.last.stats.segmentUncompressedKeysSize + updatedKeyValues.last.stats.segmentValuesSize)

      updatedKeyValues foreach {
        keyValue =>
          newUpdatedSegment.get(keyValue.key).assertGet shouldBe keyValue
      }
    }

    "merge existing segment file with new KeyValues returning new segment file with updated KeyValues" in {
      val keyValues1 = randomIntKeyValues(count = 10)
      val segment1 = TestSegment(keyValues1).assertGet

      val keyValues2 = Slice.create[KeyValue](10)
      keyValues1 foreach {
        keyValue =>
          keyValues2 add KeyValue(keyValue.key, value = randomInt(), previous = keyValues2.lastOption, falsePositiveRate = 0.1)
      }

      val segment2 = TestSegment(keyValues2).assertGet

      val mergedSegments = segment1.put(segment2.getAll(0.1).assertGet, 4.mb, 0.1).assertGet
      mergedSegments.size shouldBe 1
      val mergedSegment = mergedSegments.head

      //test merged segment should contain all
      keyValues2 foreach {
        keyValue =>
          (mergedSegment get keyValue.key).assertGet shouldBe keyValue
      }

      mergedSegment.getAll(0.1).assertGet.size shouldBe keyValues2.size
    }

    "return no new segments if all the KeyValues in the Segment were deleted and if remove deletes is true" in {
      val keyValues = randomIntKeyValues(count = 10)
      val segment = TestSegment(keyValues, removeDeletes = true).assertGet

      val deleteKeyValues = Slice.create[Delete](keyValues.size)
      keyValues.foreach(keyValue => deleteKeyValues add Delete(keyValue.key))

      segment.put(deleteKeyValues, 4.mb, 0.1).assertGet shouldBe empty
    }

    "return 1 new segment with only 1 key-value if all the KeyValues in the Segment were deleted but 1" in {
      val keyValues = randomIntKeyValues(count = 10)
      val segment = TestSegment(keyValues, removeDeletes = true).assertGet

      val deleteKeyValues = Slice.create[Delete](keyValues.size - 1)
      keyValues.drop(1).foreach(keyValue => deleteKeyValues add Delete(keyValue.key))

      val newSegments = segment.put(deleteKeyValues, 4.mb, 0.1).assertGet
      newSegments.size shouldBe 1
      newSegments.head.getKeyValueCount().assertGet shouldBe 1

      val newSegment = newSegments.head
      val keyValue = keyValues.head

      newSegment.get(keyValue.key).assertGet shouldBe keyValue

      newSegment.lower(keyValue.key).assertGetOpt shouldBe empty
      newSegment.higher(keyValue.key).assertGetOpt shouldBe empty
    }

    "distribute new Segments to multiple folders equally" in {
      val keyValues1 = Slice(KeyValue(1, 1), KeyValue(2, 2), KeyValue(3, 3), KeyValue(4, 4), KeyValue(5, 5), KeyValue(6, 6)).updateStats
      val segment = TestSegment(keyValues1).assertGet

      val keyValues2 = Slice(KeyValue(7, 7), KeyValue(8, 8), KeyValue(9, 9), KeyValue(10, 10), KeyValue(11, 11), KeyValue(12, 12)).updateStats

      val dirs = (1 to 6) map (_ => Dir(createRandomIntDirectory, 1))

      val distributor = PathsDistributor(dirs, () => Seq(segment))
      val segments =
        if (persistent)
          segment.put(keyValues2, 33.bytes, 0.1, distributor).assertGet
        else
          segment.put(keyValues2, 16.bytes, 0.1, distributor).assertGet

      //all returned segments contain all the KeyValues
      segments should have size 6
      segments(0).getAll(0.1).assertGet shouldBe(keyValues1.slice(0, 1).unslice(), ignoreStats = true)
      segments(1).getAll(0.1).assertGet shouldBe(keyValues1.slice(2, 3).unslice(), ignoreStats = true)
      segments(2).getAll(0.1).assertGet shouldBe(keyValues1.slice(4, 5).unslice(), ignoreStats = true)
      segments(3).getAll(0.1).assertGet shouldBe(keyValues2.slice(0, 1).unslice(), ignoreStats = true)
      segments(4).getAll(0.1).assertGet shouldBe(keyValues2.slice(2, 3).unslice(), ignoreStats = true)
      segments(5).getAll(0.1).assertGet shouldBe(keyValues2.slice(4, 5).unslice(), ignoreStats = true)

      //all the paths are used to write Segments
      segments(0).path.getParent shouldBe dirs(0).path
      segments(1).path.getParent shouldBe dirs(1).path
      segments(2).path.getParent shouldBe dirs(2).path
      segments(3).path.getParent shouldBe dirs(3).path
      segments(4).path.getParent shouldBe dirs(4).path

      //all paths are used
      distributor.queuedPaths shouldBe empty
    }
  }
}