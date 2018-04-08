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

import swaydb.core.data.Memory.Put
import swaydb.core.data.Transient.Remove
import swaydb.core.data.{Memory, Value, _}
import swaydb.core.io.file.DBFile
import swaydb.core.io.reader.Reader
import swaydb.core.level.PathsDistributor
import swaydb.core.map.serializer.RangeValueSerializers._
import swaydb.core.segment.SegmentException.CannotCopyInMemoryFiles
import swaydb.core.segment.format.one.{SegmentReader, SegmentWriter}
import swaydb.core.util.FileUtil._
import swaydb.core.util._
import swaydb.core.{LimitQueues, TestBase, TestLimitQueues}
import swaydb.data.config.Dir
import swaydb.data.segment.MaxKey
import swaydb.data.segment.MaxKey.{Fixed, Range}
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.order.KeyOrder
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.Random

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

    "un-slice Segment's minKey & maxKey and also un-slice cache key-values" in {
      //assert that all key-values added to cache are not sub-slices.
      def assertCacheKeyValuesAreNotSubSlices(segment: Segment) = {
        segment.cache.asScala foreach {
          case (key, value: KeyValue.ReadOnly) =>
            key.underlyingArraySize shouldBe 1

            value match {
              case fixed: Memory.Fixed =>
                fixed match {
                  case Put(key, value) =>
                    key.underlyingArraySize shouldBe 1
                    value.foreach(_.underlyingArraySize shouldBe 1)
                  case Memory.Remove(key) =>
                    key.underlyingArraySize shouldBe 1
                }

              case Memory.Range(fromKey, toKey, fromValue, rangeValue) =>
                fromKey.underlyingArraySize shouldBe 1
                toKey.underlyingArraySize shouldBe 1

                fromValue foreach {
                  case Value.Put(value) =>
                    value.foreach(_.underlyingArraySize shouldBe 1)
                  case Value.Remove =>
                }

                rangeValue match {
                  case Value.Put(value) =>
                    value.foreach(_.underlyingArraySize shouldBe 1)
                  case Value.Remove =>
                }

              case fixed: Persistent.Fixed =>
                fixed match {
                  case Persistent.Put(key, value, _, _, _, _, _) =>
                    key.underlyingArraySize shouldBe 1
                    fixed.getOrFetchValue.assertGetOpt.foreach(_.underlyingArraySize shouldBe 1)
                  case Persistent.Remove(key, _, _, _) =>
                    key.underlyingArraySize shouldBe 1

                }

              case range @ Persistent.Range(id, fromKey, toKey, _, _, _, _, _, _) =>
                fromKey.underlyingArraySize shouldBe 1
                toKey.underlyingArraySize shouldBe 1
                val (fromValue, rangeValue) = range.fetchFromAndRangeValue.assertGet
                fromValue foreach {
                  case Value.Put(value) =>
                    value.foreach(_.underlyingArraySize shouldBe 1)
                  case Value.Remove =>
                }
                rangeValue match {
                  case Value.Put(value) =>
                    value.foreach(_.underlyingArraySize shouldBe 1)
                  case Value.Remove =>
                }
            }
        }
      }

      def assertMinAndMaxKeyAreNotSubSlices(segment: Segment) = {
        segment.minKey.underlyingArraySize shouldBe 1
        segment.maxKey match {
          case Fixed(maxKey) =>
            maxKey.underlyingArraySize shouldBe 1

          case Range(fromKey, maxKey) =>
            fromKey.underlyingArraySize shouldBe 1
            maxKey.underlyingArraySize shouldBe 1
        }
      }

      def assertKeyValuesAreSubSlices(keyValues: Slice[Memory],
                                      bytes: Slice[Byte]) =
        keyValues foreach {
          readKeyValue =>
            readKeyValue.key.underlyingArraySize should be > readKeyValue.key.unslice().size
            readKeyValue.key.underlyingArraySize shouldBe bytes.size

            readKeyValue match {
              case Memory.Range(fromKey, toKey, fromValue, rangeValue) =>
                fromKey.underlyingArraySize should be > readKeyValue.key.unslice().size
                fromKey.underlyingArraySize shouldBe bytes.size

                toKey.underlyingArraySize should be > readKeyValue.key.unslice().size
                toKey.underlyingArraySize shouldBe bytes.size
              case _ =>
            }
        }

      def doAssert(keyValues: Slice[KeyValue.WriteOnly]) = {
        val bytes = SegmentWriter.toSlice(keyValues, 0.1).assertGet

        //read key-values so they are all part of the same byte array.
        val readKeyValues: Slice[Memory] = SegmentReader.readAll(SegmentReader.readFooter(Reader(bytes)).assertGet, Reader(bytes)).assertGet.toMemory

        //assert that readKeyValues keys are sub=slices of original large byte array.
        assertKeyValuesAreSubSlices(readKeyValues, bytes)

        //Create Segment with sub-slice key-values and assert min & maxKey and also check that cached key-values are un-sliced.
        assertOnSegment(
          keyValues = readKeyValues,
          assertion =
            segment => {
              assertMinAndMaxKeyAreNotSubSlices(segment)
              //if Persistent Segment, read all key-values from disk so that they get added to cache.
              if (persistent) assertGet(readKeyValues, segment)
              //assert key-values added to cache are un-sliced
              assertCacheKeyValuesAreNotSubSlices(segment)
            }
        )
      }

      doAssert(Slice(Transient.Put("a", "a"), Transient.Remove("b"), Transient.Range[Value, Value]("c", "d", Some(Value.Put("c")), Value.Put("d")), Transient.Put("d", "d")).updateStats)
      doAssert(Slice(Transient.Put("a", "a"), Transient.Range[Value, Value]("b", "d", None, Value.Remove), Transient.Put("e", "e"), Transient.Remove("f")).updateStats)
      doAssert(Slice(Transient.Put("a", "a"), Transient.Put("b", "b"), Transient.Put("c", "c"), Transient.Range[Value, Value]("d", "z", Some(Value.Put("d")), Value.Remove)).updateStats)
      doAssert(Slice(Transient.Range[Value, Value]("a", "b", None, Value.Put("a")), Transient.Put("b", "b"), Transient.Put("c", "c"), Transient.Range[Value, Value]("d", "z", Some(Value.Put("d")), Value.Put("d"))).updateStats)
    }

    "not create bloomFilter if the Segment has Remove range key-values and set hasRange to true" in {

      def doAssert(segment: Segment) = {
        segment.getBloomFilter.assertGetOpt shouldBe empty
        segment.hasRange.assertGet shouldBe true
        segment.close.assertGet
      }

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
    }

    "create bloomFilter if the Segment has no Remove range key-values by has update range key-values and set hasRange to true when there are Range key-value" in {
      assertOnSegment(
        keyValues = Slice(Memory.Put(0), Memory.Put(1, 1), Memory.Remove(2)),
        assertion =
          segment => {
            segment.getBloomFilter.assertGetOpt shouldBe defined
            segment.hasRange.assertGet shouldBe false
            segment.close.assertGet
          }
      )

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

    "initialise a segment that already exists but Segment info is unknown" in {
      if (memory) {
        //memory Segments cannot re-initialise Segments after shutdown.
      } else {
        val keyValues = randomIntKeyValues(keyValuesCount, addRandomDeletes = true, addRandomRanges = true)
        val segmentFile = testSegmentFile

        val segment = TestSegment(keyValues, path = segmentFile).assertGet
        val readSegment = Segment(segment.path, Random.nextBoolean(), Random.nextBoolean(), false, true).assertGet

        segment shouldBe readSegment
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
    "open a closed Segment on read and clear footer" in {
      val keyValues = randomIntKeyValues(keyValuesCount)
      val segment = TestSegment(keyValues).assertGet

      def close = {
        segment.close.assertGet
        if (levelStorage.persistent) {
          segment.isFileDefined shouldBe false
          segment.isOpen shouldBe false
          segment.isFooterDefined shouldBe false
        }
      }

      def open(keyValue: KeyValue.WriteOnly) = {
        segment.get(keyValue.key).assertGet shouldBe keyValue
        segment.isFileDefined shouldBe true
        segment.isOpen shouldBe true
        segment.isFooterDefined shouldBe true
      }

      keyValues foreach {
        keyValue =>
          close
          open(keyValue)
      }
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

      segment1.getKeyValueCount().assertGet shouldBe keyValues.size
      segment1.isOpen shouldBe true

      //create another segment should close segment 1
      val segment2 = TestSegment(keyValues)(ordering, keyValueLimiterImplicit, segmentOpenLimit).assertGet
      segment2.getKeyValueCount().assertGet shouldBe keyValues.size

      eventual(5.seconds) {
        //segment one is closed
        segment1.isOpen shouldBe false
      }
      //read one key value from Segment1 so that it's reopened and added to the cache. This will also remove Segment 2 from cache
      (segment1 get keyValues.head.key).assertGet shouldBe keyValues.head
      segment1.isOpen shouldBe true

      eventual(5.seconds) {
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
      segment.isFooterDefined shouldBe true //footer is set in-memory

      segment.delete.assertGet
      segment.cacheSize shouldBe keyValues.size //cache is not cleared
      if (persistent) {
        segment.isOpen shouldBe false
        segment.isFooterDefined shouldBe false //on delete in-memory footer is cleared
      }
      segment.existsOnDisk shouldBe false
    }
  }

  "Segment.copyTo" should {
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

        segment.put(newKeyValues.toMemory, 1.kb, 0.1).failed.assertGet shouldBe a[FileAlreadyExistsException]

        //the folder should contain only the original segment and the segmentToFailPut
        segment.path.getParent.files(Extension.Seg) should contain only(segment.path, segmentToFailPut.path)
      }
    }

    "return new segment with deleted KeyValues if all keys were deleted and removeDeletes is false" in {
      val keyValues = Slice(Transient.Put(1), Transient.Put(2), Transient.Put(3), Transient.Put(4), Transient.Range[Value, Value](5, 10, None, Value.Put(10))).updateStats
      val segment = TestSegment(keyValues, removeDeletes = false).assertGet
      assertGet(keyValues, segment)

      val deleteKeyValues = Slice(Memory.Remove(1), Memory.Remove(2), Memory.Remove(3), Memory.Remove(4), Memory.Range(5, 10, None, Value.Remove))

      val deletedSegment = segment.put(deleteKeyValues, 4.mb, 0.1).assertGet
      deletedSegment should have size 1

      val newDeletedSegment = deletedSegment.head
      newDeletedSegment.getKeyValueCount().assertGet shouldBe keyValues.size

      assertGet(keyValues, segment)
      if (persistent) assertGet(keyValues, segment.reopen)
    }

    "return new segment with updated KeyValues if all keys values were updated to None" in {
      val keyValues = randomIntKeyStringValues(count = 100, addRandomRanges = true)
      val segment = TestSegment(keyValues, removeDeletes = true).assertGet

      val updatedKeyValues = Slice.create[Memory](keyValues.size)
      keyValues.foreach(keyValue => updatedKeyValues add Memory.Put(keyValue.key))

      val updatedSegments = segment.put(updatedKeyValues, 4.mb, 0.1).assertGet
      updatedSegments should have size 1

      val newUpdatedSegment = updatedSegments.head
      newUpdatedSegment.getKeyValueCount().assertGet shouldBe keyValues.size

      assertGet(updatedKeyValues, newUpdatedSegment)
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
      val keyValues = Slice(Transient.Put(1), Transient.Put(2), Transient.Put(3), Transient.Put(4), Transient.Range[Value, Value](5, 10, None, Value.Put(10))).updateStats
      val segment = TestSegment(keyValues, removeDeletes = true).assertGet

      val deleteKeyValues = Slice.create[Memory](keyValues.size)
      (1 to 4).foreach(key => deleteKeyValues add Memory.Remove(key))
      deleteKeyValues add Memory.Range(5, 10, None, Value.Remove)

      segment.put(deleteKeyValues, 4.mb, 0.1).assertGet shouldBe empty
    }

    "slice Put range into slice with fromValue set to Remove" in {
      val keyValues = Slice(Transient.Range[Value, Value](1, 10, None, Value.Put(10))).updateStats
      val segment = TestSegment(keyValues, removeDeletes = false).assertGet

      val deleteKeyValues = Slice.create[Memory](10)
      (1 to 10).foreach(key => deleteKeyValues add Memory.Remove(key))

      val removedRanges = segment.put(deleteKeyValues, 4.mb, 0.1).assertGet.head.getAll().assertGet

      val expected: Seq[Memory] = (1 to 9).map(key => Memory.Range(key, key + 1, Some(Value.Remove), Value.Put(10))) :+ Memory.Remove(10)

      removedRanges shouldBe expected
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