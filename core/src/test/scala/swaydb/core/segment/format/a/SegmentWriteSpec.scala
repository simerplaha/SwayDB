/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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
 */

package swaydb.core.segment.format.a

import java.nio.file._
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.util.Random
import swaydb.configs.level.DefaultGroupingStrategy
import swaydb.core.CommonAssertions._
import swaydb.core.IOAssert._
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.data.Transient.Remove
import swaydb.core.data.Value.{FromValue, RangeValue}
import swaydb.core.data.{Memory, Value, _}
import swaydb.core.group.compression.data.KeyValueGroupingStrategyInternal
import swaydb.core.io.file.IOEffect._
import swaydb.core.io.reader.Reader
import swaydb.core.level.PathsDistributor
import swaydb.core.queue.FileLimiter
import swaydb.core.segment.Segment
import swaydb.core.segment.SegmentException.CannotCopyInMemoryFiles
import swaydb.core.segment.merge.SegmentMerger
import swaydb.core.util._
import swaydb.core.{TestBase, TestData, TestLimitQueues, TestTimer}
import swaydb.data.config.Dir
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.util.ByteSizeOf
import swaydb.data.util.StorageUnits._
import swaydb.data.{IO, MaxKey}
import swaydb.serializers.Default._
import swaydb.serializers._

//@formatter:off
class SegmentWriteSpec0 extends SegmentWriteSpec

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

sealed trait SegmentWriteSpec extends TestBase with Benchmark {

  val keyValuesCount = 100

  implicit def testTimer: TestTimer = TestTimer.random

  implicit def groupingStrategy: Option[KeyValueGroupingStrategyInternal] =
    randomGroupingStrategyOption(keyValuesCount)

  implicit val keyOrder = KeyOrder.default
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  implicit val keyValueLimiter = TestLimitQueues.keyValueLimiter

  //  override def deleteFiles = false

  implicit val fileOpenLimiter: FileLimiter = TestLimitQueues.fileOpenLimiter

  "Segment" should {

    "create a Segment" in {
      runThis(100.times) {
        assertSegment(
          keyValues =
            randomizedKeyValues(randomIntMax(keyValuesCount) max 1).toMemory,

          assert =
            (keyValues, segment) => {
              assertReads(keyValues, segment)
              segment.minKey shouldBe keyValues.head.key
              segment.maxKey shouldBe {
                keyValues.last match {
                  case _: Memory.Fixed =>
                    MaxKey.Fixed[Slice[Byte]](keyValues.last.key)

                  case group: Memory.Group =>
                    group.maxKey

                  case range: Memory.Range =>
                    MaxKey.Range[Slice[Byte]](range.fromKey, range.toKey)
                }
              }

              //ensure that min and max keys are slices
              segment.minKey.underlyingArraySize shouldBe 4
              segment.maxKey match {
                case MaxKey.Fixed(maxKey) =>
                  maxKey.underlyingArraySize shouldBe 4

                case MaxKey.Range(fromKey, maxKey) =>
                  fromKey.underlyingArraySize shouldBe 4
                  maxKey.underlyingArraySize shouldBe 4
              }

              if (keyValues.toTransient.last.stats.hasRemoveRange)
                segment.getBloomFilter.assertGetOpt shouldBe empty
              else
                assertBloom(keyValues, segment.getBloomFilter.assertGet)

              segment.close.assertGet
            }
        )
      }
    }

    "set minKey & maxKey to be Fixed if the last key-value is a Fixed key-value" in {
      runThis(100.times) {
        assertSegment(
          keyValues =
            Slice(randomRangeKeyValue(1, 10), randomFixedKeyValue(11)),
          assert =
            (keyValues, segment) => {
              segment.minKey shouldBe (1: Slice[Byte])
              segment.maxKey shouldBe MaxKey.Fixed[Slice[Byte]](11)
              segment.minKey.underlyingArraySize shouldBe ByteSizeOf.int
              segment.maxKey.maxKey.underlyingArraySize shouldBe ByteSizeOf.int
              segment.close.assertGet
            }
        )
      }
    }

    "set minKey & maxKey to be Range if the last key-value is a Range key-value" in {
      runThis(100.times) {
        assertSegment(
          keyValues = Slice(randomFixedKeyValue(0), randomRangeKeyValue(1, 10)),
          assert =
            (keyValues, segment) => {
              segment.minKey shouldBe (0: Slice[Byte])
              segment.maxKey shouldBe MaxKey.Range[Slice[Byte]](1, 10)
              segment.close.assertGet
            }
        )
      }
    }

    "set minKey & maxKey to be Range if the last key-value is a Group and the Group's last key-value is Range" in {
      runThis(100.times) {
        assertSegment(
          keyValues = Slice(randomFixedKeyValue(0), randomGroup(Slice(randomFixedKeyValue(2), randomRangeKeyValue(5, 10)).toTransient)).toMemory,
          assert =
            (keyValues, segment) => {
              segment.minKey shouldBe (0: Slice[Byte])
              segment.maxKey shouldBe MaxKey.Range[Slice[Byte]](5, 10)
              segment.minKey.underlyingArraySize shouldBe ByteSizeOf.int

              val rangeMaxKey = segment.maxKey.asInstanceOf[MaxKey.Range[Slice[Byte]]]
              rangeMaxKey.maxKey.underlyingArraySize shouldBe ByteSizeOf.int
              rangeMaxKey.fromKey.underlyingArraySize shouldBe ByteSizeOf.int

              segment.close.assertGet
            }
        )
      }
    }

    "set minKey & maxKey to be Range if last key-value is a Group and the Group's last key-value is Fixed" in {
      runThis(10.times) {
        assertSegment(
          keyValues =
            Slice(
              randomFixedKeyValue(0),
              randomGroup(Slice(randomRangeKeyValue(5, 10),
                randomFixedKeyValue(20)).toTransient)
            ).toMemory,
          assert =
            (keyValues, segment) => {
              segment.minKey shouldBe (0: Slice[Byte])
              segment.maxKey shouldBe MaxKey.Fixed[Slice[Byte]](20)
              segment.close.assertGet
            }
        )
      }
    }

    "set minKey & maxKey to be Range if the last key-value is a Group and the Group's last key-value is also another Group with range last key-Value" in {
      runThis(10.times) {
        assertSegment(
          keyValues =
            Slice(
              randomFixedKeyValue(0),
              randomGroup(
                Slice(
                  randomFixedKeyValue(2).toTransient,
                  randomGroup(Slice(randomFixedKeyValue(3), randomRangeKeyValue(5, 10)).toTransient)
                ).updateStats
              )
            ).toMemory,
          assert =
            (keyValues, segment) => {
              segment.minKey shouldBe (0: Slice[Byte])
              segment.maxKey shouldBe MaxKey.Range[Slice[Byte]](5, 10)
              segment.close.assertGet
            }
        )
      }
    }

    "un-slice Segment's minKey & maxKey and also un-slice cache key-values" in {
      //assert that all key-values added to cache are not sub-slices.
      def assertCacheKeyValuesAreSliced(segment: Segment) =
        segment.cache.asScala foreach {
          case (key, value: KeyValue.ReadOnly) =>
            key.shouldBeSliced()
            assertSliced(value)
        }

      def assertMinAndMaxKeyAreSliced(segment: Segment) = {
        segment.minKey.underlyingArraySize shouldBe 1
        segment.maxKey match {
          case MaxKey.Fixed(maxKey) =>
            maxKey.underlyingArraySize shouldBe 1

          case MaxKey.Range(fromKey, maxKey) =>
            fromKey.underlyingArraySize shouldBe 1
            maxKey.underlyingArraySize shouldBe 1
        }
      }

      def doAssert(keyValues: Slice[KeyValue.WriteOnly]) = {
        val (bytes, _) = SegmentWriter.write(keyValues, 0, false, TestData.falsePositiveRate).assertGet

        //read key-values so they are all part of the same byte array.
        val readKeyValues: Slice[Memory] = SegmentReader.readAll(SegmentReader.readFooter(Reader(bytes)).assertGet, Reader(bytes)).assertGet.toMemory

        //assert that readKeyValues keys are not sliced.
        readKeyValues foreach assertNotSliced

        //Create Segment with sub-slice key-values and assert min & maxKey and also check that cached key-values are un-sliced.
        assertSegment(
          keyValues = readKeyValues,
          assert =
            (keyValues, segment) => {
              assertMinAndMaxKeyAreSliced(segment)
              //if Persistent Segment, read all key-values from disk so that they get added to cache.
              if (persistent) assertGet(readKeyValues, segment)
              //assert key-values added to cache are un-sliced
              assertCacheKeyValuesAreSliced(segment)
            }
        )
      }

      runThis(10.times) {
        //unique data to avoid compression.
        //this test asserts that key-values are not sliced unnecessarily.
        val data = ('a' to 'z').toArray
        var index = 0

        def nextData: String = {
          val next = data(index)
          index += 1
          next.toString
        }

        val uninitialisedKeyValues =
          List(
            () => randomFixedKeyValue(nextData, Some(nextData)),
            () => randomFixedKeyValue(nextData, Some(nextData)),
            () => randomRangeKeyValue(nextData, nextData, randomFromValueOption(Some(nextData)), randomRangeValue(Some(nextData))),
            () => randomGroup(Slice(randomFixedKeyValue(nextData, Some(nextData)).toTransient)).toMemory
          )

        doAssert(
          Random.shuffle(uninitialisedKeyValues ++ uninitialisedKeyValues).map(_ ()).toTransient
        )
      }
    }

    "not create bloomFilter if the Segment has Remove range key-values or function key-values and set hasRange to true" in {

      def doAssert(keyValues: Slice[KeyValue], segment: Segment) = {
        segment.getBloomFilter.assertGetOpt shouldBe empty
        segment.hasRange.assertGet shouldBe true
        segment.close.assertGet
      }

      assertSegment(
        keyValues = Slice(Memory.put(0), Memory.Range(1, 10, None, Value.remove(randomDeadlineOption, Time.empty))),
        assert = doAssert
      )

      assertSegment(
        keyValues = Slice(Memory.put(0), Memory.Range(1, 10, Some(Value.remove(None, Time.empty)), Value.remove(randomDeadlineOption, Time.empty))),
        assert = doAssert
      )

      assertSegment(
        keyValues = Slice(Memory.put(0), Memory.Range(1, 10, Some(Value.update(None, randomDeadlineOption, Time.empty)), Value.remove(randomDeadlineOption, Time.empty))),
        assert = doAssert
      )

      assertSegment(
        keyValues = Slice(Memory.put(0), Memory.Range(1, 10, Some(Value.put(Some(1), randomDeadlineOption, Time.empty)), Value.remove(randomDeadlineOption, Time.empty))),
        assert = doAssert
      )

      //      assertSegment(
      //        keyValues = Slice(Memory.put(0), Memory.Range(1, 10, Some(Value.PendingApply(Some(1), randomDeadlineOption, Time.empty)), Value.remove(randomDeadlineOption, Time.empty))),
      //        assert = doAssert
      //      )

      //group can also have a range key-value which should have the same effect.

      runThis(100.times) {
        assertSegment(
          keyValues =
            Slice(
              Memory.put(0),
              randomGroup(
                Slice(
                  eitherOne(
                    Memory.Range(1, 10, Some(Value.put(Some(1), randomDeadlineOption, Time.empty)), Value.remove(randomDeadlineOption, Time.empty)),
                    Memory.Range(1, 10, Some(Value.put(Some(1), randomDeadlineOption, Time.empty)), Value.Function(randomFunctionId(), Time.empty)),
                    Memory.Range(
                      fromKey = 1,
                      toKey = 10,
                      fromValue = Some(Value.put(Some(1), randomDeadlineOption, Time.empty)),
                      rangeValue =
                        Value.PendingApply(
                          applies =
                            eitherOne(
                              left = Slice(randomFunctionValue()),
                              right = Slice(randomRemoveFunctionValue())
                            )
                        )
                    )
                  )
                ).toTransient
              ).toMemory
            ),
          assert = doAssert
        )
      }
    }

    "create bloomFilter if the Segment has no Remove range key-values but has update range key-values. And set hasRange to true" in {
      assertSegment(
        keyValues = Slice(Memory.put(0), Memory.put(1, 1), Memory.remove(2, randomDeadlineOption)),
        assert =
          (keyValues, segment) => {
            segment.getBloomFilter.assertGetOpt shouldBe defined
            segment.hasRange.assertGet shouldBe false
            segment.close.assertGet
          }
      )

      assertSegment(
        keyValues = Slice(Memory.put(0), Memory.Range(1, 10, None, Value.update(10, randomDeadlineOption))),
        assert =
          (keyValues, segment) => {
            segment.getBloomFilter.assertGetOpt shouldBe defined
            segment.hasRange.assertGet shouldBe true
            segment.close.assertGet
          }
      )

      assertSegment(
        keyValues =
          Slice(
            Memory.put(0),
            randomGroup(Slice(Memory.Range(1, 10, Some(Value.put(Some(1), randomDeadlineOption, Time.empty)), Value.update(1, randomDeadlineOption))).toTransient).toMemory
          ),
        assert =
          (keyValues, segment) => {
            segment.getBloomFilter.assertGetOpt shouldBe defined
            segment.hasRange.assertGet shouldBe true
            segment.close.assertGet
          }
      )
    }

    "set hasRange to true if the Segment contains Range key-values" in {

      def doAssert(keyValues: Slice[KeyValue], segment: Segment): Unit = {
        segment.hasRange.assertGet shouldBe true
        segment.hasPut.assertGet shouldBe true
        segment.close.assertGet
      }

      assertSegment(
        keyValues = Slice(Memory.put(0), Memory.Range(1, 10, None, Value.update(10))),
        assert = doAssert
      )

      assertSegment(
        keyValues = Slice(Memory.put(0), Memory.Range(1, 10, Some(Value.remove(None, Time.empty)), Value.update(10))),
        assert = doAssert
      )

      assertSegment(
        keyValues = Slice(Memory.put(0), Memory.Range(1, 10, Some(Value.put(1)), Value.update(10))),
        assert = doAssert
      )

      assertSegment(
        keyValues = Slice(Memory.put(0), Memory.Range(1, 10, None, Value.remove(None, Time.empty))),
        assert = doAssert
      )

      assertSegment(
        keyValues = Slice(Memory.put(0), Memory.Range(1, 10, Some(Value.remove(10.seconds.fromNow)), Value.remove(None, Time.empty))),
        assert = doAssert
      )

      assertSegment(
        keyValues = Slice(Memory.put(0), Memory.Range(1, 10, Some(Value.put(1)), Value.remove(None, Time.empty))),
        assert = doAssert
      )

      runThisParallel(100.times) {
        assertSegment(
          keyValues = Slice(Memory.put(0), randomRangeKeyValue(1, 10)),
          assert = doAssert
        )
      }

      assertSegment(
        keyValues = randomPutKeyValues(keyValuesCount, addRandomRemoves = true, addRandomRanges = true, addRandomPutDeadlines = true, addRandomRemoveDeadlines = true),
        assert = doAssert
      )

      assertSegment(
        keyValues = Slice(randomGroup(Slice(Memory.Range(1, 10, Some(Value.put(Some(1), randomDeadlineOption, Time.empty)), Value.update(1, randomDeadlineOption))).toTransient).toMemory),
        assert = doAssert
      )
    }

    "not overwrite a Segment if it already exists" in {
      if (memory) {
        //memory Segments do not check for overwrite. No tests required
      } else {
        assertSegment(
          keyValues =
            randomPutKeyValues(keyValuesCount),
          assert =
            (keyValues, segment) => {
              val failedKeyValues = randomKeyValues(keyValuesCount, addRandomRemoves = true)
              TestSegment(failedKeyValues, path = segment.path).failed.assertGet.exception shouldBe a[FileAlreadyExistsException]
              //data remained unchanged
              assertReads(keyValues, segment)
              failedKeyValues foreach {
                keyValue =>
                  segment.get(keyValue.key).assertGetOpt.isEmpty shouldBe true
              }
              assertBloom(keyValues, segment.getBloomFilter.assertGet)
            }
        )
      }
    }

    "initialise a segment that already exists" in {
      if (memory) {
        //memory Segments cannot re-initialise Segments after shutdown.
      } else {
        runThisParallel(10.times) {
          assertSegment(
            keyValues = randomizedKeyValues(keyValuesCount),
            closeAfterCreate = true,
            testWithCachePopulated = false,
            assert =
              (keyValues, segment) => {
                segment.isOpen shouldBe false
                segment.isFileDefined shouldBe false
                segment.isCacheEmpty shouldBe true

                assertReads(keyValues, segment)

                segment.isOpen shouldBe true
                segment.isFileDefined shouldBe true
                segment.isCacheEmpty shouldBe false

                segment.getBloomFilter.assertGetOpt.foreach(bloom => assertBloom(keyValues, bloom))

                segment.close.assertGet
              }
          )
        }
      }
    }

    "initialise a segment that already exists but Segment info is unknown" in {
      if (memory) {
        //memory Segments cannot re-initialise Segments after shutdown.
      } else {
        runThis(10.times) {
          assertSegment(
            keyValues = randomizedKeyValues(keyValuesCount),
            assert =
              (keyValues, segment) => {
                val readSegment = Segment(segment.path, randomBoolean(), randomBoolean(), false, true).assertGet
                readSegment shouldBe segment
              }
          )
        }
      }
    }

    "fail initialisation if the segment does not exist" in {
      if (memory) {
        //memory Segments do not get re-initialised
      } else {
        val segment = TestSegment().assertGet
        segment.delete.assertGet

        segment.tryReopen.failed.assertGet.exception shouldBe a[NoSuchFileException]
      }
    }
  }

  "deleteSegments" should {
    "delete multiple segments" in {
      val segment1 = TestSegment(randomizedKeyValues(keyValuesCount)).assertGet
      val segment2 = TestSegment(randomizedKeyValues(keyValuesCount)).assertGet
      val segment3 = TestSegment(randomizedKeyValues(keyValuesCount)).assertGet

      val deleted = Segment.deleteSegments(Seq(segment1, segment2, segment3))
      deleted.assertGet shouldBe 3

      //files should be closed
      segment1.isOpen shouldBe false
      segment2.isOpen shouldBe false
      segment3.isOpen shouldBe false

      segment1.isFileDefined shouldBe false
      segment2.isFileDefined shouldBe false
      segment3.isFileDefined shouldBe false

      segment1.existsOnDisk shouldBe false
      segment2.existsOnDisk shouldBe false
      segment3.existsOnDisk shouldBe false
    }
  }

  "Segment" should {
    "open a closed Segment on read and clear footer" in {
      runThis(10.times) {
        implicit val fileOpenLimiter: FileLimiter = FileLimiter.empty

        val keyValues = randomizedKeyValues(keyValuesCount)
        val segment = TestSegment(keyValues).assertGet

        def close = {
          segment.close.assertGet
          if (levelStorage.persistent) {
            //also clear the cache so that if the key-value is a group on open file is still reopened
            //instead of just reading from in-memory Group key-value.
            segment.clearCache()
            segment.isFileDefined shouldBe false
            segment.isOpen shouldBe false
            segment.isFooterDefined shouldBe false
          }
        }

        def open(keyValue: KeyValue) = {
          segment.get(keyValue.key).assertGet shouldBe keyValue
          segment.isFileDefined shouldBe true
          segment.isOpen shouldBe true
          segment.isFooterDefined shouldBe true
        }

        unzipGroups(keyValues) foreach {
          keyValue =>
            close
            open(keyValue)
        }
        //finally also close the segment to close the file.
        close
      }
    }

    "fail read and write operations on a Segment that does not exists" in {
      val keyValues = randomizedKeyValues(keyValuesCount)
      val segment = TestSegment(keyValues).assertGet

      segment.delete.assertGet
      segment.isOpen shouldBe false
      segment.isFileDefined shouldBe false

      segment.existsOnDisk shouldBe false
      segment.get(keyValues.head.key).failed.assertGet.exception shouldBe a[NoSuchFileException]
      segment.put(keyValues.toMemory, 1.mb, TestData.falsePositiveRate, true).failed.assertGet.exception shouldBe a[NoSuchFileException]
      segment.refresh(1.mb, TestData.falsePositiveRate, true).failed.assertGet.exception shouldBe a[NoSuchFileException]
      segment.isOpen shouldBe false
      segment.isFileDefined shouldBe false
    }
  }

  "reopen closed channel for read when closed by LimitQueue" in {
    if (memory) {
      //memory Segments do not get closed via
    } else {
      implicit val segmentOpenLimit = FileLimiter(1, 100.millisecond)
      val keyValues = randomizedKeyValues(keyValuesCount, addRandomGroups = false)
      val segment1 = TestSegment(keyValues)(keyOrder, keyValueLimiter, segmentOpenLimit).assertGet

      segment1.getHeadKeyValueCount().assertGet shouldBe keyValues.size
      segment1.isOpen shouldBe true

      //create another segment should close segment 1
      val segment2 = TestSegment(keyValues)(keyOrder, keyValueLimiter, segmentOpenLimit).assertGet
      segment2.getHeadKeyValueCount().assertGet shouldBe keyValues.size

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

  "delete" should {
    "close the channel and delete the file" in {
      val keyValues = randomizedKeyValues(keyValuesCount)
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

  "copyTo" should {
    "copy the segment to a target path without deleting the original" in {
      if (memory) {
        val segment = TestSegment(randomizedKeyValues(keyValuesCount)).assertGet
        segment.copyTo(randomFilePath).failed.assertGet.exception shouldBe CannotCopyInMemoryFiles(segment.path)
      } else {
        val keyValues = randomizedKeyValues(keyValuesCount)
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

  "copyToPersist" should {
    "copy the segment and persist it to disk" in {
      val keyValues = randomizedKeyValues(keyValuesCount)
      val segment = TestSegment(keyValues).assertGet
      val levelPath = createNextLevelPath
      val segments =
        Segment.copyToPersist(
          segment = segment,
          createdInLevel = 0,
          fetchNextPath = levelPath.resolve(nextSegmentId),
          mmapSegmentsOnRead = levelStorage.mmapSegmentsOnRead,
          mmapSegmentsOnWrite = levelStorage.mmapSegmentsOnWrite,
          compressDuplicateValues = true,
          removeDeletes = false,
          minSegmentSize =
            if (persistent)
              keyValues.last.stats.segmentSize / 10
            else
              keyValues.last.stats.memorySegmentSize / 10,
          bloomFilterFalsePositiveRate = TestData.falsePositiveRate
        ).assertGet

      if (persistent)
        segments should have size 1
      else
        segments.size should be > 2

      segments.foreach(_.existsOnDisk shouldBe true)
      Segment.getAllKeyValues(TestData.falsePositiveRate, segments).assertGet shouldBe keyValues
    }

    "copy the segment and persist it to disk when remove deletes is true" in {
      runThis(10.times) {
        val keyValues = randomizedKeyValues(keyValuesCount)
        val segment = TestSegment(keyValues).assertGet
        val levelPath = createNextLevelPath

        val segments =
          Segment.copyToPersist(
            segment = segment,
            createdInLevel = 0,
            fetchNextPath = levelPath.resolve(nextSegmentId),
            mmapSegmentsOnRead = levelStorage.mmapSegmentsOnRead,
            mmapSegmentsOnWrite = levelStorage.mmapSegmentsOnWrite,
            removeDeletes = true,
            compressDuplicateValues = true,
            minSegmentSize =
              if (persistent)
                keyValues.last.stats.segmentSize / 10
              else
                keyValues.last.stats.memorySegmentSize / 10,
            bloomFilterFalsePositiveRate = TestData.falsePositiveRate
          ).assertGet

        segments.foreach(_.existsOnDisk shouldBe true)

        if (persistent)
          unzipGroups(Segment.getAllKeyValues(TestData.falsePositiveRate, segments).assertGet) shouldBe unzipGroups(keyValues) //persistent Segments are simply copied and are not checked for removed key-values.
        else
          unzipGroups(Segment.getAllKeyValues(TestData.falsePositiveRate, segments).assertGet) shouldBe unzipGroups(keyValues).collect { //memory Segments does a split/merge and apply lastLevel rules.
            case keyValue: Transient.Put if keyValue.hasTimeLeft() =>
              keyValue
            case Transient.Range(fromKey, _, _, Some(put @ Value.Put(_, deadline, _)), _, _, _, _) if deadline.forall(_.hasTimeLeft()) =>
              put.toMemory(fromKey).toTransient
          }.updateStats
      }
    }

    "revert copy if Segment initialisation fails after copy" in {
      val keyValues = randomizedKeyValues(keyValuesCount)
      val segment = TestSegment(keyValues).assertGet
      val levelPath = createNextLevelPath
      val nextPath = levelPath.resolve(nextSegmentId)

      Files.createFile(nextPath) //path already taken.

      Segment.copyToPersist(
        segment = segment,
        createdInLevel = 0,
        fetchNextPath = nextPath,
        mmapSegmentsOnRead = levelStorage.mmapSegmentsOnRead,
        mmapSegmentsOnWrite = levelStorage.mmapSegmentsOnWrite,
        removeDeletes = true,
        compressDuplicateValues = true,
        minSegmentSize =
          if (persistent)
            keyValues.last.stats.segmentSize / 10
          else
            keyValues.last.stats.memorySegmentSize / 10,
        bloomFilterFalsePositiveRate = TestData.falsePositiveRate
      ).failed.assertGet.exception shouldBe a[FileAlreadyExistsException]

      Files.size(nextPath) shouldBe 0
      if (persistent) segment.existsOnDisk shouldBe true //original Segment remains untouched

    }

    "revert copy of Key-values if creating at least one Segment fails" in {
      val keyValues = randomizedKeyValues(keyValuesCount).toMemory
      val levelPath = createNextLevelPath
      val nextSegmentId = nextId

      def nextPath = levelPath.resolve(IDGenerator.segmentId(nextId))

      Files.createFile(levelPath.resolve(IDGenerator.segmentId(nextSegmentId + 4))) //path already taken.

      levelStorage.dirs foreach {
        dir =>
          Files.createDirectories(dir.path)
          IO(Files.createFile(dir.path.resolve(IDGenerator.segmentId(nextSegmentId + 4)))) //path already taken.
      }

      val filesBeforeCopy = levelPath.files(Extension.Seg)
      filesBeforeCopy.size shouldBe 1

      Segment.copyToPersist(
        keyValues = keyValues,
        createdInLevel = 0,
        fetchNextPath = nextPath,
        mmapSegmentsOnRead = levelStorage.mmapSegmentsOnRead,
        mmapSegmentsOnWrite = levelStorage.mmapSegmentsOnWrite,
        removeDeletes = false,
        minSegmentSize = keyValues.toTransient.last.stats.segmentSize / 5,
        bloomFilterFalsePositiveRate = TestData.falsePositiveRate,
        compressDuplicateValues = true

      ).failed.assertGet.exception shouldBe a[FileAlreadyExistsException]

      levelPath.files(Extension.Seg) shouldBe filesBeforeCopy
    }
  }

  "copyToMemory" should {
    "copy persistent segment and store it in Memory" in {

      val keyValues = randomizedKeyValues(keyValuesCount)
      val segment = TestSegment(keyValues).assertGet
      val levelPath = createNextLevelPath
      val segments =
        Segment.copyToMemory(
          segment = segment,
          fetchNextPath = levelPath.resolve(nextSegmentId),
          removeDeletes = false,
          compressDuplicateValues = true,
          minSegmentSize =
            if (persistent)
              keyValues.last.stats.segmentSize / 4
            else
              keyValues.last.stats.memorySegmentSize / 4,
          bloomFilterFalsePositiveRate = TestData.falsePositiveRate
        ).assertGet

      segments.size should be >= 2 //ensures that splits occurs. Memory Segments do not get written to disk without splitting.

      segments.foreach(_.existsOnDisk shouldBe false)
      Segment.getAllKeyValues(TestData.falsePositiveRate, segments).assertGet shouldBe keyValues
    }

    "copy the segment and persist it to disk when removeDeletes is true" in {
      runThis(10.times) {
        val keyValues = randomizedKeyValues(1000)
        val segment = TestSegment(keyValues).assertGet
        val levelPath = createNextLevelPath

        val segments =
          Segment.copyToMemory(
            segment = segment,
            fetchNextPath = levelPath.resolve(nextSegmentId),
            removeDeletes = true,
            compressDuplicateValues = true,
            minSegmentSize = keyValues.last.stats.segmentSize / 1000, //divide by large because key-value can contain all expired.
            bloomFilterFalsePositiveRate = TestData.falsePositiveRate
          ).assertGet

        segments.foreach(_.existsOnDisk shouldBe false)

        segments.size should be >= 2 //ensures that splits occurs. Memory Segments do not get written to disk without splitting.

        //some key-values could get expired while unexpired key-values are being collected. So try again!
        IO {
          Segment.getAllKeyValues(TestData.falsePositiveRate, segments).assertGet shouldBe unzipGroups(keyValues).collect {
            case keyValue: Transient.Put if keyValue.hasTimeLeft() =>
              keyValue
            case Transient.Range(fromKey, _, _, Some(put @ Value.Put(_, deadline, _)), _, _, _, _) if deadline.forall(_.hasTimeLeft()) =>
              put.toMemory(fromKey).toTransient
          }.updateStats
        }
      }
    }

  }

  "put" should {
    "return None for empty byte arrays for values" in {
      runThis(10.times) {
        val keyValuesWithEmptyValues = ListBuffer.empty[Memory]

        (1 to 100).foldLeft(0) {
          case (i, _) =>
            var key = i

            def nextKey = {
              key += 1
              key
            }

            keyValuesWithEmptyValues +=
              eitherOne(
                left = randomFixedKeyValue(nextKey, Some(Slice.empty)),
                mid = randomRangeKeyValue(nextKey, nextKey, randomFromValueOption(Some(Slice.emptyBytes)), randomRangeValue(Some(Slice.emptyBytes))),
                right = randomFixedKeyValue(nextKey, Some(Slice.empty))
              )
            key
        }

        val segment = TestSegment(keyValuesWithEmptyValues.toTransient).assertGet

        def valuesValueShouldBeNone(value: Value): Unit =
          value match {
            case Value.Update(value, deadline, time) =>
              value shouldBe None
            case Value.Put(value, deadline, time) =>
              value shouldBe None
            case Value.PendingApply(applies) =>
              applies foreach valuesValueShouldBeNone
            case Value.Function(function, time) =>
            //nothing to assert
            case Value.Remove(deadline, time) =>
            //nothing to assert
          }

        segment.getAll().assertGet foreach {
          case keyValue: KeyValue.ReadOnly.Put =>
            keyValue.getOrFetchValue.assertGetOpt shouldBe None

          case keyValue: KeyValue.ReadOnly.Update =>
            keyValue.getOrFetchValue.assertGetOpt shouldBe None

          case keyValue: KeyValue.ReadOnly.Range =>
            val (fromValue, rangeValue) = keyValue.fetchFromAndRangeValue.assertGet
            Seq(fromValue, Some(rangeValue)).flatten foreach valuesValueShouldBeNone

          case apply: KeyValue.ReadOnly.PendingApply =>
            apply.getOrFetchApplies.assertGet foreach valuesValueShouldBeNone

          case _: KeyValue.ReadOnly.Function =>
          //nothing to assert
          case _: KeyValue.ReadOnly.Remove =>
          //nothing to assert
        }
      }
    }

    "reopen closed channel" in {
      val keyValues1 = randomizedKeyValues(keyValuesCount)

      val segment = TestSegment(keyValues1).assertGet
      segment.close.assertGet
      if (persistent) segment.isOpen shouldBe false

      val keyValues2 = randomizedKeyValues(keyValuesCount)
      segment.put(keyValues2, 1.mb, TestData.falsePositiveRate, true).assertGet
      if (persistent) segment.isOpen shouldBe true
    }

    "return a new segment with merged key values" in {
      val keyValues = Slice(Transient.put(1, 1))
      val segment = TestSegment(keyValues).assertGet

      val newKeyValues = Slice(Memory.put(2, 2))
      val newSegments = segment.put(newKeyValues, 4.mb, TestData.falsePositiveRate, true).assertGet
      newSegments should have size 1

      val allReadKeyValues = Segment.getAllKeyValues(TestData.falsePositiveRate, newSegments).assertGet

      val expectedKeyValues = SegmentMerger.merge(newKeyValues, keyValues.toMemory, 1.mb, false, forInMemory = memory, TestData.falsePositiveRate, true).assertGet
      expectedKeyValues should have size 1

      allReadKeyValues shouldBe expectedKeyValues.head
    }

    "return multiple new segments with merged key values" in {
      val keyValues = randomizedKeyValues(10000)
      val segment = TestSegment(keyValues).get

      val newKeyValues = randomizedKeyValues(10000)
      val newSegments = segment.put(newKeyValues.toMemory, 10.kb, TestData.falsePositiveRate, true).assertGet
      newSegments.size should be > 1

      val allReadKeyValues = Segment.getAllKeyValues(TestData.falsePositiveRate, newSegments).assertGet

      //give merge a very large size so that there are no splits (test convenience)
      val expectedKeyValues = SegmentMerger.merge(newKeyValues.toMemory, keyValues.toMemory, 10.mb, false, forInMemory = memory, TestData.falsePositiveRate, true).assertGet
      expectedKeyValues should have size 1

      //allReadKeyValues are read from multiple Segments so valueOffsets will be invalid so stats will be invalid
      allReadKeyValues shouldBe expectedKeyValues.head
    }

    "fail put and delete partially written batch Segments if there was a failure in creating one of them" in {
      if (memory) {
        // not for in-memory Segments
      } else {

        val keyValues = randomizedKeyValues(keyValuesCount)
        val segment = TestSegment(keyValues).get
        val newKeyValues = randomizedKeyValues(10000)

        val tenthSegmentId = {
          val segmentId = (segment.path.fileId.get._1 + 10).toSegmentFileId
          segment.path.getParent.resolve(segmentId)
        }

        //create a segment with the next id in sequence which should fail put with FileAlreadyExistsException
        val segmentToFailPut = TestSegment(path = tenthSegmentId).assertGet

        segment.put(newKeyValues.toMemory, 1.kb, TestData.falsePositiveRate, true).failed.assertGet.exception shouldBe a[FileAlreadyExistsException]

        //the folder should contain only the original segment and the segmentToFailPut
        segment.path.getParent.files(Extension.Seg) should contain only(segment.path, segmentToFailPut.path)
      }
    }

    "return new segment with deleted KeyValues if all keys were deleted and removeDeletes is false" in {
      implicit def testTimer: TestTimer = TestTimer.Empty

      val keyValues = Slice(
        Transient.put(1),
        Transient.put(2),
        Transient.put(3),
        Transient.put(4),
        Transient.Range.create[FromValue, RangeValue](5, 10, None, Value.Update(None, None, testTimer.next))
      ).updateStats
      val segment = TestSegment(keyValues, removeDeletes = false).assertGet
      assertGet(keyValues, segment)

      val deleteKeyValues = Slice(Memory.remove(1), Memory.remove(2), Memory.remove(3), Memory.remove(4), Memory.Range(5, 10, None, Value.remove(None)))

      val deletedSegment = segment.put(deleteKeyValues, 4.mb, TestData.falsePositiveRate, true).assertGet
      deletedSegment should have size 1
      val newDeletedSegment = deletedSegment.head
      unzipGroups(newDeletedSegment.getAll().assertGet) shouldBe deleteKeyValues

      assertGet(keyValues, segment)
      if (persistent) assertGet(keyValues, segment.reopen)
    }

    "return new segment with updated KeyValues if all keys values were updated to None" in {
      implicit val testTimer: TestTimer = TestTimer.Incremental()

      val keyValues = randomizedKeyValues(count = keyValuesCount, addRandomGroups = false)
      val segment = TestSegment(keyValues, removeDeletes = true).assertGet

      val updatedKeyValues = Slice.create[Memory](keyValues.size)
      keyValues.foreach(keyValue => updatedKeyValues add Memory.put(keyValue.key, None))

      val updatedSegments = segment.put(updatedKeyValues, 4.mb, TestData.falsePositiveRate, true).assertGet
      updatedSegments should have size 1

      val newUpdatedSegment = updatedSegments.head
      unzipGroups(newUpdatedSegment.getAll().assertGet) shouldBe updatedKeyValues

      assertGet(updatedKeyValues, newUpdatedSegment)
    }

    "merge existing segment file with new KeyValues returning new segment file with updated KeyValues" in {
      runThis(10.times) {
        implicit val testTimer: TestTimer = TestTimer.Incremental()
        //ranges get split to make sure there are no ranges.
        val keyValues1 = randomizedKeyValues(count = keyValuesCount, addRandomRanges = false)
        val segment1 = TestSegment(keyValues1).assertGet

        val keyValues2Unclosed = Slice.create[Transient](keyValues1.size * 100)
        unzipGroups(keyValues1) foreach {
          keyValue =>
            keyValues2Unclosed add randomPutKeyValue(keyValue.key).toTransient
        }

        val keyValues2Closed = keyValues2Unclosed.close().updateStats

        val segment2 = TestSegment(keyValues2Closed).assertGet

        val mergedSegments = segment1.put(segment2.getAll().assertGet.toSlice, 10.mb, TestData.falsePositiveRate, true).assertGet
        mergedSegments.size shouldBe 1
        val mergedSegment = mergedSegments.head

        //test merged segment should contain all
        keyValues2Closed foreach {
          keyValue =>
            (mergedSegment get keyValue.key).assertGet shouldBe keyValue
        }

        unzipGroups(mergedSegment.getAll().assertGet).size shouldBe keyValues2Closed.size
      }
    }

    "return no new segments if all the KeyValues in the Segment were deleted and if remove deletes is true" in {
      val keyValues =
        Slice(
          randomFixedKeyValue(1),
          randomFixedKeyValue(2),
          randomFixedKeyValue(3),
          randomFixedKeyValue(4),
          randomRangeKeyValue(5, 10, Some(randomRangeValue()), randomRangeValue()),
          randomGroup(Slice(randomFixedKeyValue(11), randomRangeKeyValue(12, 15, Some(randomRangeValue()), randomRangeValue())).toTransient).toMemory
        ).toTransient

      val segment = TestSegment(keyValues, removeDeletes = true).assertGet

      val deleteKeyValues = Slice.create[Memory](keyValues.size)
      (1 to 4).foreach(key => deleteKeyValues add Memory.remove(key))
      deleteKeyValues add Memory.Range(5, 10, None, Value.remove(None))
      deleteKeyValues add Memory.Range(11, 15, None, Value.remove(None))

      segment.put(deleteKeyValues, 4.mb, TestData.falsePositiveRate, true).assertGet shouldBe empty
    }

    "slice Put range into slice with fromValue set to Remove" in {
      implicit val testTimer: TestTimer = TestTimer.Empty

      val keyValues = Slice(Transient.Range.create[FromValue, RangeValue](1, 10, None, Value.update(10))).updateStats
      val segment = TestSegment(keyValues, removeDeletes = false).assertGet

      val deleteKeyValues = Slice.create[Memory](10)
      (1 to 10).foreach(key => deleteKeyValues add Memory.remove(key))

      val removedRanges = segment.put(deleteKeyValues, 4.mb, TestData.falsePositiveRate, true).assertGet.head.getAll().assertGet

      val expected: Seq[Memory] = (1 to 9).map(key => Memory.Range(key, key + 1, Some(Value.remove(None)), Value.update(10))) :+ Memory.remove(10)

      removedRanges shouldBe expected
    }

    "return 1 new segment with only 1 key-value if all the KeyValues in the Segment were deleted but 1" in {
      implicit val testTimer: TestTimer = TestTimer.Empty

      val keyValues = randomKeyValues(count = keyValuesCount)
      val segment = TestSegment(keyValues, removeDeletes = true).assertGet

      val deleteKeyValues = Slice.create[Remove](keyValues.size - 1)
      keyValues.drop(1).foreach(keyValue => deleteKeyValues add Transient.remove(keyValue.key))

      val newSegments = segment.put(deleteKeyValues.toMemory, 4.mb, TestData.falsePositiveRate, true).assertGet
      newSegments.size shouldBe 1
      newSegments.head.getHeadKeyValueCount().assertGet shouldBe 1

      val newSegment = newSegments.head
      val keyValue = keyValues.head

      newSegment.get(keyValue.key).assertGet shouldBe keyValue

      newSegment.lower(keyValue.key).assertGetOpt shouldBe empty
      newSegment.higher(keyValue.key).assertGetOpt shouldBe empty
    }

    "distribute new Segments to multiple folders equally" in {
      implicit val groupingStrategy: Option[KeyValueGroupingStrategyInternal] = None

      val keyValues1 = Slice(Transient.put(1, 1), Transient.put(2, 2), Transient.put(3, 3), Transient.put(4, 4), Transient.put(5, 5), Transient.put(6, 6)).updateStats
      val segment = TestSegment(keyValues1).assertGet

      val keyValues2 = Slice(Memory.put(7, 7), Memory.put(8, 8), Memory.put(9, 9), Memory.put(10, 10), Memory.put(11, 11), Memory.put(12, 12))

      val dirs = (1 to 6) map (_ => Dir(createRandomIntDirectory, 1))

      val distributor = PathsDistributor(dirs, () => Seq(segment))
      val segments =
        if (persistent)
          segment.put(keyValues2, 60.bytes, TestData.falsePositiveRate, true, distributor).assertGet
        else
          segment.put(keyValues2, 21.bytes, TestData.falsePositiveRate, true, distributor).assertGet

      //all returned segments contain all the KeyValues ???
      //      segments should have size 6
      //      segments(0).getAll().assertGet shouldBe keyValues1.slice(0, 1).unslice()
      //      segments(1).getAll().assertGet shouldBe keyValues1.slice(2, 3).unslice()
      //      segments(2).getAll().assertGet shouldBe keyValues1.slice(4, 5).unslice()
      //      segments(3).getAll().assertGet shouldBe keyValues2.slice(0, 1).unslice()
      //      segments(4).getAll().assertGet shouldBe keyValues2.slice(2, 3).unslice()
      //      segments(5).getAll().assertGet shouldBe keyValues2.slice(4, 5).unslice()

      //all the paths are used to write Segments
      segments(0).path.getParent shouldBe dirs(0).path
      segments(1).path.getParent shouldBe dirs(1).path
      segments(2).path.getParent shouldBe dirs(2).path
      segments(3).path.getParent shouldBe dirs(3).path
      segments(4).path.getParent shouldBe dirs(4).path

      //all paths are used ???
      //      distributor.queuedPaths shouldBe empty
    }
  }

  "refresh" should {
    "return new Segment with Removed key-values removed" in {
      if (persistent) {
        val keyValues1 = (1 to 100).map(key => eitherOne(randomRemoveKeyValue(key), randomRangeKeyValue(key, key + 1, None, randomRangeValue()))).toTransient
        val segment = TestSegment(keyValues1).assertGet
        segment.getAll().assertGet shouldBe keyValues1

        val reopened = segment.reopen(segment.path, removeDeletes = true)
        reopened.getBloomFilterKeyValueCount().assertGet shouldBe keyValues1.size
        reopened.refresh(1.mb, TestData.falsePositiveRate, true).assertGet shouldBe empty
      }
    }

    "return no new Segments if all the key-values in the Segment were expired" in {
      val keyValues1 = (1 to 100).map(key => randomPutKeyValue(key, deadline = Some(1.second.fromNow))).toTransient
      val segment = TestSegment(keyValues1, removeDeletes = true).assertGet
      segment.getHeadKeyValueCount().assertGet shouldBe keyValues1.size

      sleep(2.seconds)
      segment.refresh(1.mb, TestData.falsePositiveRate, true).assertGet shouldBe empty
    }

    "return all key-values when removeDeletes is false" in {
      val keyValues1 = (1 to 100).map(key => Transient.put(key, key, 1.second)).updateStats
      val segment = TestSegment(keyValues1, removeDeletes = false).assertGet
      segment.getHeadKeyValueCount().assertGet shouldBe keyValues1.size

      sleep(2.seconds)
      val refresh = segment.refresh(1.mb, TestData.falsePositiveRate, true).assertGet
      refresh should have size 1
      refresh.head shouldContainAll keyValues1
    }
  }

  "split & then write" should {
    "succeed for non group key-values" in {
      implicit val groupingStrategy: Option[KeyValueGroupingStrategyInternal] = None
      val keyValues = randomizedKeyValues(1000, addRandomGroups = false)
      val result = SegmentMerger.split(keyValues, 100.mb, false, inMemoryStorage, TestData.falsePositiveRate, true).assertGet
      result should have size 1
      result.head should have size keyValues.size
      val (bytes, deadline) = SegmentWriter.write(result.head, 0, false, TestData.falsePositiveRate).assertGet
      readAll(bytes).assertGet shouldBe keyValues
    }

    "succeed for grouped key-values" in {
      val keyValues = randomizedKeyValues(1000)
      val result = SegmentMerger.split(
        keyValues = keyValues,
        minSegmentSize = 100.mb,
        isLastLevel = false,
        forInMemory = inMemoryStorage,
        bloomFilterFalsePositiveRate = TestData.falsePositiveRate,
        compressDuplicateValues = true
      )(keyOrder, Some(KeyValueGroupingStrategyInternal(DefaultGroupingStrategy()))).assertGet
      result should have size 1
      result.head should have size 1

      val (bytes, deadline) = SegmentWriter.write(result.head, 0, false, TestData.falsePositiveRate).assertGet
      readAll(bytes).assertGet shouldBe keyValues
    }
  }
}
