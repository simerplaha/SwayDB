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

import swaydb.core.data.Transient.Remove
import swaydb.core.data.{Memory, _}
import swaydb.core.io.file.{DBFile, IO}
import swaydb.core.level.PathsDistributor
import swaydb.data.segment.MaxKey.{Fixed, Range}
import swaydb.core.segment.SegmentException.CannotCopyInMemoryFiles
import swaydb.core.util.FileUtil._
import swaydb.core.util._
import swaydb.core.{LimitQueues, TestBase, TestLimitQueues}
import swaydb.data.config.Dir
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.order.KeyOrder
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.Random
import swaydb.core.map.serializer.RangeValueSerializers._
import swaydb.data.segment.MaxKey

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

  implicit val ordering: Ordering[Slice[Byte]] = KeyOrder.default
  val keyValuesCount = 100

  //  override def deleteFiles = false

  implicit val fileOpenLimiterImplicit: DBFile => Unit = TestLimitQueues.fileOpenLimiter
  implicit val keyValueLimiterImplicit: (Persistent, Segment) => Unit = TestLimitQueues.keyValueLimiter

  "Segment" should {

    "create a Segment" in {
      assertOnSegment(
        keyValues =
          randomIntKeyValuesMemory(keyValuesCount, addRandomDeletes = Random.nextBoolean(), addRandomRanges = Random.nextBoolean()),
        assertionWithKeyValues =
          (keyValues, segment) => {
            assertReads(keyValues, segment)
            segment.minKey shouldBe keyValues.head.key
            segment.maxKey shouldBe {
              keyValues.last match {
                case _: Memory.Fixed =>
                  MaxKey.Fixed(keyValues.last.key)
                case range: Memory.Range =>
                  MaxKey.Range(range.fromKey, range.toKey)
              }
            }

            //ensure that min and max keys are slices
            segment.minKey.underlyingArraySize shouldBe 4
            segment.maxKey match {
              case Fixed(maxKey) =>
                maxKey.underlyingArraySize shouldBe 4
              case Range(fromKey, maxKey) =>
                fromKey.underlyingArraySize shouldBe 4
                maxKey.underlyingArraySize shouldBe 4
            }

            if (segment.getBloomFilter.assertGetOpt.isDefined)
              keyValues foreach {
                keyValue =>
                  segment.getBloomFilter.assertGet.mightContain(keyValue.key) shouldBe true
              }

            segment.close.assertGet
          }
      )
    }

    "set maxKey to be Fixed if the last key-value is a Fixed key-value" in {
      assertOnSegment(
        keyValues =
          Slice(Memory.Range(1, 10, None, Value.Remove), Memory.Put(11)),
        assertion =
          segment => {
            segment.maxKey shouldBe MaxKey.Fixed(11)
            segment.close.assertGet
          }
      )
    }

    "set maxKey to be Range if the last key-value is a Range key-value" in {
      assertOnSegment(
        keyValues = Slice(Memory.Put(0), Memory.Range(1, 10, None, Value.Remove)),
        assertion =
          segment => {
            segment.maxKey shouldBe MaxKey.Range(1, 10)
            segment.close.assertGet
          }
      )

      assertOnSegment(
        keyValues = Slice(Memory.Put(0), Memory.Range(1, 10, None, Value.Put(10))),
        assertion =
          segment => {
            segment.maxKey shouldBe MaxKey.Range(1, 10)
            segment.close.assertGet
          }
      )

      assertOnSegment(
        keyValues = Slice(Memory.Put(0), Memory.Range(1, 10, Some(Value.Remove), Value.Put(10))),
        assertion =
          segment => {
            segment.maxKey shouldBe MaxKey.Range(1, 10)
            segment.close.assertGet
          }
      )
    }

    "not create bloomFilter if the Segment has Remove range key-values and footer should set hasRange to true" in {

      assertOnSegment(
        keyValues = Slice(Memory.Put(0), Memory.Range(1, 10, None, Value.Remove)),
        assertion =
          segment => {
            segment.getBloomFilter.assertGetOpt shouldBe empty
            segment.hasRange.assertGet shouldBe true
            segment.close.assertGet
          }
      )
    }

    "create bloomFilter if the Segment has no Remove range key-values by has update range key-values and footer should set hasRange to true" in {
      assertOnSegment(
        keyValues = Slice(Memory.Put(0), Memory.Range(1, 10, None, Value.Put(10))),
        assertion =
          segment => {
            segment.getBloomFilter.assertGetOpt shouldBe defined
            segment.hasRange.assertGet shouldBe true
            segment.close.assertGet
          }
      )
    }

    "set hasRange to true if the Segment contains Range key-values" in {

      def doAssert(segment: Segment): Unit = {
        segment.hasRange.assertGet shouldBe true
        segment.close.assertGet
      }

      assertOnSegment(
        keyValues = Slice(Memory.Put(0), Memory.Range(1, 10, None, Value.Put(10))),
        assertion = doAssert(_)
      )

      assertOnSegment(
        keyValues = Slice(Memory.Put(0), Memory.Range(1, 10, Some(Value.Remove), Value.Put(10))),
        assertion = doAssert(_)
      )

      assertOnSegment(
        keyValues = Slice(Memory.Put(0), Memory.Range(1, 10, Some(Value.Put(1)), Value.Put(10))),
        assertion = doAssert(_)
      )

      assertOnSegment(
        keyValues = Slice(Memory.Put(0), Memory.Range(1, 10, None, Value.Remove)),
        assertion = doAssert(_)
      )

      assertOnSegment(
        keyValues = Slice(Memory.Put(0), Memory.Range(1, 10, Some(Value.Remove), Value.Remove)),
        assertion = doAssert(_)
      )

      assertOnSegment(
        keyValues = Slice(Memory.Put(0), Memory.Range(1, 10, Some(Value.Put(1)), Value.Remove)),
        assertion = doAssert(_)
      )

      assertOnSegment(
        keyValues = randomIntKeyValuesMemory(keyValuesCount, addRandomDeletes = true, addRandomRanges = true),
        assertion = doAssert(_)
      )
    }

    "create a Segment and populate cache" in {
      val keyValues = Slice(Transient.Put("a", 1), Transient.Put("b", 2), Transient.Put("c", 3), Transient.Put("d", 4), Transient.Put("e", 5), Transient.Put("f", 6)).updateStats
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
        //memory Segments do not check for overwrite. No tests required
      } else {
        val keyValues = randomIntKeyValues(keyValuesCount, addRandomDeletes = true)
        val failedKeyValues = randomIntKeyValues(keyValuesCount, addRandomDeletes = true)
        val segment = TestSegment(keyValues).assertGet

        TestSegment(failedKeyValues, path = segment.path).failed.assertGet shouldBe a[FileAlreadyExistsException]

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

        val segment = TestSegment(keyValues, path = segmentFile).assertGet
        val reopenedSegment = segment.reopen

        //ensure that Segments opened for reads and lazily loaded.
        reopenedSegment.isOpen shouldBe false
        reopenedSegment.isFileDefined shouldBe false
        reopenedSegment.isCacheEmpty shouldBe true
        assertReads(keyValues, reopenedSegment)
        reopenedSegment.isOpen shouldBe true
        reopenedSegment.isFileDefined shouldBe true
        reopenedSegment.isCacheEmpty shouldBe false

        assertBloom(keyValues, reopenedSegment.getBloomFilter.assertGet)
      }
    }

    "fail initialisation if the segment does not exist" in {
      if (memory) {
        //memory Segments do not get re-initialised
      } else {
        val segment = TestSegment().assertGet
        segment.delete.assertGet

        segment.tryReopen.failed.assertGet shouldBe a[NoSuchFileException]
      }
    }
  }

  "Segment.deleteSegments" should {
    "delete multiple segments" in {
      val segment1 = TestSegment(randomIntKeyValues(keyValuesCount, addRandomDeletes = true, addRandomRanges = true)).assertGet
      val segment2 = TestSegment(randomIntKeyStringValues(keyValuesCount, addRandomDeletes = true, addRandomRanges = true)).assertGet
      val segment3 = TestSegment(randomIntKeyStringValues(keyValuesCount, addRandomDeletes = true, addRandomRanges = true)).assertGet

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
    }
  }

  "Segment" should {
    "open a closed Segment on read" in {
      val keyValues = randomIntKeyValues(keyValuesCount)
      val segment = TestSegment(keyValues).assertGet

      def close = {
        segment.close.assertGet
        if (levelStorage.persistent) {
          segment.isFileDefined shouldBe false
          segment.isOpen shouldBe false
        }
      }

      def open(keyValue: KeyValue.WriteOnly) = {
        segment.get(keyValue.key).assertGet shouldBe keyValue
        segment.isFileDefined shouldBe true
        segment.isOpen shouldBe true
      }

      close
      open(keyValues.head)

      close
      open(keyValues.last)

    }

    "fail get and put operations on a Segment that does not exists" in {
      val keyValues = randomIntKeyValues(keyValuesCount, addRandomDeletes = true)
      val segment = TestSegment(keyValues).assertGet

      segment.delete.assertGet
      segment.isOpen shouldBe false
      segment.isFileDefined shouldBe false

      segment.existsOnDisk shouldBe false
      segment.get(keyValues.head.key).failed.assertGet shouldBe a[NoSuchFileException]
      segment.put(keyValues.toMemory, 1.mb, 0.1).failed.assertGet shouldBe a[NoSuchFileException]
      segment.isOpen shouldBe false
      segment.isFileDefined shouldBe false
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
    "close the channel and delete the file" in {
      val keyValues = randomIntKeyStringValues(keyValuesCount, addRandomDeletes = true)
      val segment = TestSegment(keyValues).get
      assertReads(keyValues, segment) //populate the cache

      segment.cacheSize shouldBe keyValues.size

      segment.delete.isSuccess shouldBe true
      segment.cacheSize shouldBe keyValues.size //cache is not cleared
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
        val keyValuesReadOnly = keyValues

        val segment = TestSegment(keyValues).get
        val targetPath = createRandomIntDirectory.resolve(nextId + s".${Extension.Seg}")

        segment.copyTo(targetPath).assertGet
        segment.existsOnDisk shouldBe true

        val copiedSegment = segment.reopen(targetPath)
        copiedSegment.getAll().assertGet shouldBe keyValuesReadOnly
        copiedSegment.path shouldBe targetPath

        //original segment should still exist
        segment.getAll().assertGet shouldBe keyValuesReadOnly
      }
    }
  }

  "Segment.put" should {
    "return None for empty values" in {
      val segment =
        TestSegment(
          Slice(
            Transient.Put(1, Slice.create[Byte](0)),
            Transient.Range[Value, Value](2, 3, None, Value.Put(Slice.create[Byte](0))),
            Transient.Range[Value, Value](3, 4, Some(Value.Put(Slice.create[Byte](0))), Value.Put(Slice.create[Byte](0))),
            Transient.Range[Value, Value](4, 5, Some(Value.Remove), Value.Put(Slice.create[Byte](0))),
            Transient.Range[Value, Value](5, 6, Some(Value.Put(Slice.create[Byte](0))), Value.Remove)
          ).updateStats
        ).assertGet

      segment.get(1).assertGet.getOrFetchValue.assertGetOpt shouldBe None

      val range2 = segment.get(2).assertGet.asInstanceOf[KeyValue.ReadOnly.Range]
      range2.fetchFromValue.assertGetOpt shouldBe empty
      range2.fetchRangeValue.assertGet shouldBe Value.Put(None)

      val range3 = segment.get(3).assertGet.asInstanceOf[KeyValue.ReadOnly.Range]
      range3.fetchFromValue.assertGet shouldBe Value.Put(None)
      range3.fetchRangeValue.assertGet shouldBe Value.Put(None)

      val range4 = segment.get(4).assertGet.asInstanceOf[KeyValue.ReadOnly.Range]
      range4.fetchFromValue.assertGet shouldBe Value.Remove
      range4.fetchRangeValue.assertGet shouldBe Value.Put(None)

      val range5 = segment.get(5).assertGet.asInstanceOf[KeyValue.ReadOnly.Range]
      range5.fetchFromValue.assertGet shouldBe Value.Put(None)
      range5.fetchRangeValue.assertGet shouldBe Value.Remove
    }

    "reopen closed channel" in {
      val keyValue1 = Slice(Transient.Put(5, Slice(randomBytes(100))))

      val segment = TestSegment(keyValue1).assertGet
      segment.close.assertGet
      if (persistent) segment.isOpen shouldBe false

      val keyValue2 = Slice(Memory.Put(2, Slice(randomBytes(100))), Memory.Put(3, Slice(randomBytes(100))))
      segment.put(keyValue2, 1.mb, 0.1).assertGet
      if (persistent) segment.isOpen shouldBe true
    }

    "return a new segment with merged key values" in {
      val keyValues = Slice(Transient.Put(1, 1))
      val segment = TestSegment(keyValues).assertGet

      val newKeyValues = Slice(Memory.Put(2, 2))
      val newSegments = segment.put(newKeyValues, 4.mb, 0.1).assertGet
      newSegments should have size 1

      val allReadKeyValues = Segment.getAllKeyValues(0.1, newSegments).assertGet

      val expectedKeyValues = SegmentMerge.merge(newKeyValues, keyValues.toMemory, 1.mb, false, forInMemory = memory, 0.1).assertGet
      expectedKeyValues should have size 1

      allReadKeyValues.map(entry => (entry.key, entry.getOrFetchValue.assertGetOpt)) shouldBe expectedKeyValues.head.map(entry => (entry.key, entry.getOrFetchValue.assertGetOpt))
    }

    "return multiple new segments with merged key values" in {
      val keyValues = randomIntKeyStringValues(10000, addRandomDeletes = true, addRandomRanges = true)
      val segment = TestSegment(keyValues).get

      val newKeyValues = randomIntKeyValues(10000, addRandomDeletes = true, addRandomRanges = true)
      val newSegments = segment.put(newKeyValues.toMemory, 10.kb, 0.1).assertGet
      newSegments.size should be > 1

      val allReadKeyValues = Segment.getAllKeyValues(0.1, newSegments).assertGet

      //give merge a very large size so that there are no splits (test convenience)
      val expectedKeyValues = SegmentMerge.merge(newKeyValues.toMemory, keyValues.toMemory, 10.mb, false, forInMemory = memory, 0.1).assertGet
      expectedKeyValues should have size 1

      //allReadKeyValues are read from multiple Segments so valueOffsets will be invalid so stats will be invalid
      allReadKeyValues.zip(expectedKeyValues.head) foreach {
        case (segmentMergedKeyValue, manuallyMergedKeyValue) =>
          segmentMergedKeyValue shouldBe manuallyMergedKeyValue
      }
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

        segment.put(newKeyValues.toMemory, 1.kb, 0.1).failed.get shouldBe a[FileAlreadyExistsException]

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

      val deleteKeyValues = Slice.create[Memory.Remove](keyValues.size)
      keyValues.foreach(keyValue => deleteKeyValues add Memory.Remove(keyValue.key))

      val deletedSegment = segment.put(deleteKeyValues, 4.mb, 0.1).assertGet
      deletedSegment should have size 1

      val newDeletedSegment = deletedSegment.head
      newDeletedSegment.getKeyValueCount().assertGet shouldBe keyValues.size

      deleteKeyValues foreach {
        keyValue =>
          newDeletedSegment.get(keyValue.key).assertGet shouldBe keyValue
      }
    }

    "return new segment with updated KeyValues if all keys values were updated to None" in {
      val keyValues = randomIntKeyStringValues(count = 100)
      val segment = TestSegment(keyValues, removeDeletes = true).assertGet

      val updatedKeyValues = Slice.create[Memory](keyValues.size)
      keyValues.foreach(keyValue => updatedKeyValues add Memory.Put(keyValue.key))

      val updatedSegments = segment.put(updatedKeyValues, 4.mb, 0.1).assertGet
      updatedSegments should have size 1

      val newUpdatedSegment = updatedSegments.head
      newUpdatedSegment.getKeyValueCount().assertGet shouldBe keyValues.size

      updatedKeyValues foreach {
        keyValue =>
          newUpdatedSegment.get(keyValue.key).assertGet shouldBe keyValue
      }
    }

    "merge existing segment file with new KeyValues returning new segment file with updated KeyValues" in {
      val keyValues1 = randomIntKeyValues(count = 10)
      val segment1 = TestSegment(keyValues1).assertGet

      val keyValues2 = Slice.create[KeyValue.WriteOnly](10)
      keyValues1 foreach {
        keyValue =>
          keyValues2 add Transient.Put(keyValue.key, value = randomInt(), previous = keyValues2.lastOption, falsePositiveRate = 0.1)
      }

      val segment2 = TestSegment(keyValues2).assertGet

      val mergedSegments = segment1.put(segment2.getAll().assertGet.toSlice, 4.mb, 0.1).assertGet
      mergedSegments.size shouldBe 1
      val mergedSegment = mergedSegments.head

      //test merged segment should contain all
      keyValues2 foreach {
        keyValue =>
          (mergedSegment get keyValue.key).assertGet shouldBe keyValue
      }

      mergedSegment.getAll().assertGet.size shouldBe keyValues2.size
    }

    "return no new segments if all the KeyValues in the Segment were deleted and if remove deletes is true" in {
      val keyValues = randomIntKeyValues(count = 10)
      val segment = TestSegment(keyValues, removeDeletes = true).assertGet

      val deleteKeyValues = Slice.create[Memory.Remove](keyValues.size)
      keyValues.foreach(keyValue => deleteKeyValues add Memory.Remove(keyValue.key))

      segment.put(deleteKeyValues, 4.mb, 0.1).assertGet shouldBe empty
    }

    "return 1 new segment with only 1 key-value if all the KeyValues in the Segment were deleted but 1" in {
      val keyValues = randomIntKeyValues(count = 10)
      val segment = TestSegment(keyValues, removeDeletes = true).assertGet

      val deleteKeyValues = Slice.create[Remove](keyValues.size - 1)
      keyValues.drop(1).foreach(keyValue => deleteKeyValues add Remove(keyValue.key))

      val newSegments = segment.put(deleteKeyValues.toMemory, 4.mb, 0.1).assertGet
      newSegments.size shouldBe 1
      newSegments.head.getKeyValueCount().assertGet shouldBe 1

      val newSegment = newSegments.head
      val keyValue = keyValues.head

      newSegment.get(keyValue.key).assertGet shouldBe keyValue

      newSegment.lower(keyValue.key).assertGetOpt shouldBe empty
      newSegment.higher(keyValue.key).assertGetOpt shouldBe empty
    }

    "distribute new Segments to multiple folders equally" in {
      val keyValues1 = Slice(Transient.Put(1, 1), Transient.Put(2, 2), Transient.Put(3, 3), Transient.Put(4, 4), Transient.Put(5, 5), Transient.Put(6, 6)).updateStats
      val segment = TestSegment(keyValues1).assertGet

      val keyValues2 = Slice(Memory.Put(7, 7), Memory.Put(8, 8), Memory.Put(9, 9), Memory.Put(10, 10), Memory.Put(11, 11), Memory.Put(12, 12))

      val dirs = (1 to 6) map (_ => Dir(createRandomIntDirectory, 1))

      val distributor = PathsDistributor(dirs, () => Seq(segment))
      val segments =
        if (persistent)
          segment.put(keyValues2, 33.bytes, 0.1, distributor).assertGet
        else
          segment.put(keyValues2, 16.bytes, 0.1, distributor).assertGet

      //all returned segments contain all the KeyValues
      segments should have size 6
      segments(0).getAll().assertGet shouldBe keyValues1.slice(0, 1).unslice()
      segments(1).getAll().assertGet shouldBe keyValues1.slice(2, 3).unslice()
      segments(2).getAll().assertGet shouldBe keyValues1.slice(4, 5).unslice()
      segments(3).getAll().assertGet shouldBe keyValues2.slice(0, 1).unslice()
      segments(4).getAll().assertGet shouldBe keyValues2.slice(2, 3).unslice()
      segments(5).getAll().assertGet shouldBe keyValues2.slice(4, 5).unslice()

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