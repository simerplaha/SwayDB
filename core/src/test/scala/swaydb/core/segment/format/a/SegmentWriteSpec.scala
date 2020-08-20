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
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.core.segment.format.a

import java.nio.file.{FileAlreadyExistsException, NoSuchFileException}

import org.scalatest.OptionValues.convertOptionToValuable
import swaydb.Error.Segment.ExceptionHandler
import swaydb.IO
import swaydb.IOValues._
import swaydb.core.CommonAssertions._
import swaydb.data.RunThis._
import swaydb.core.TestCaseSweeper._
import swaydb.core.TestData._
import swaydb.core.actor.{ByteBufferSweeper, FileSweeper}
import swaydb.core.data.Value.FromValue
import swaydb.core.data._
import swaydb.core.io.file.Effect._
import swaydb.core.io.file.{BlockCache, Effect}
import swaydb.core.level.PathsDistributor
import swaydb.core.segment._
import swaydb.core.segment.format.a.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.format.a.block.bloomfilter.BloomFilterBlock
import swaydb.core.segment.format.a.block.hashindex.HashIndexBlock
import swaydb.core.segment.format.a.block.segment.SegmentBlock
import swaydb.core.segment.format.a.block.sortedindex.SortedIndexBlock
import swaydb.core.segment.format.a.block.values.ValuesBlock
import swaydb.core.segment.merge.{MergeStats, SegmentMerger}
import swaydb.core.util._
import swaydb.core.{TestBase, TestCaseSweeper, TestExecutionContext, TestSweeper, TestTimer}
import swaydb.data.MaxKey
import swaydb.data.config.{ActorConfig, Dir, MMAP}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.data.util.{ByteSizeOf, OperatingSystem}
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.util.Random

class SegmentWriteSpec0 extends SegmentWriteSpec

class SegmentWriteSpec1 extends SegmentWriteSpec {
  override def levelFoldersCount = 10
  override def mmapSegments = MMAP.Enabled(OperatingSystem.isWindows)
  override def level0MMAP = MMAP.Enabled(OperatingSystem.isWindows)
  override def appendixStorageMMAP = MMAP.Enabled(OperatingSystem.isWindows)
}

class SegmentWriteSpec2 extends SegmentWriteSpec {
  override def levelFoldersCount = 10
  override def mmapSegments = MMAP.Disabled
  override def level0MMAP = MMAP.Disabled
  override def appendixStorageMMAP = MMAP.Disabled
}

class SegmentWriteSpec3 extends SegmentWriteSpec {
  override def inMemoryStorage = true
}

sealed trait SegmentWriteSpec extends TestBase {

  val keyValuesCount = 100

  implicit val testTimer: TestTimer = TestTimer.Empty

  implicit val ec = TestExecutionContext.executionContext
  implicit val keyOrder = KeyOrder.default
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  implicit def segmentIO = SegmentIO.random

  "Segment" should {

    "create a Segment" in {
      runThis(100.times, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
            assertSegment(
              keyValues =
                randomizedKeyValues(eitherOne(randomIntMax(keyValuesCount) max 1, keyValuesCount)),
              //            Slice(randomPutKeyValue(1, None, None)(previous = None, sortedIndexConfig = SortedIndexBlock.Config.random.copy(disableKeyPrefixCompression = true, prefixCompressionResetCount = 0))),

              assert =
                (keyValues, segment) => {
                  assertReads(keyValues, segment)

                  segment.segmentId shouldBe Effect.fileId(segment.path)._1

                  segment.minKey shouldBe keyValues.head.key
                  segment.maxKey shouldBe {
                    keyValues.last match {
                      case _: Memory.Fixed =>
                        MaxKey.Fixed[Slice[Byte]](keyValues.last.key)

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
                  assertBloom(keyValues, segment)
                  segment.close
                }
            )
        }
      }
    }

    "set minKey & maxKey to be Fixed if the last key-value is a Fixed key-value" in {
      runThis(50.times) {
        TestCaseSweeper {
          implicit sweeper =>
            assertSegment(
              keyValues =
                Slice(randomRangeKeyValue(1, 10), randomFixedKeyValue(11)),

              assert =
                (keyValues, segment) => {
                  segment.minKey shouldBe (1: Slice[Byte])
                  segment.maxKey shouldBe MaxKey.Fixed[Slice[Byte]](11)
                  segment.minKey.underlyingArraySize shouldBe ByteSizeOf.int
                  segment.maxKey.maxKey.underlyingArraySize shouldBe ByteSizeOf.int
                  segment.close.runRandomIO.right.value
                }
            )
        }
      }
    }

    "set minKey & maxKey to be Range if the last key-value is a Range key-value" in {
      runThis(50.times) {
        TestCaseSweeper {
          implicit sweeper =>
            assertSegment(
              keyValues = Slice(randomFixedKeyValue(0), randomRangeKeyValue(1, 10)),

              assert =
                (keyValues, segment) => {
                  segment.minKey shouldBe (0: Slice[Byte])
                  segment.maxKey shouldBe MaxKey.Range[Slice[Byte]](1, 10)
                  segment.close.runRandomIO.right.value
                }
            )
        }
      }
    }

    "un-slice Segment's minKey & maxKey and also un-slice cache key-values" in {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._
          //set this MemorySweeper as root sweeper.
          TestSweeper.createMemorySweeperMax().value.sweep()

          //assert that all key-values added to cache are not sub-slices.
          def assertCacheKeyValuesAreSliced(segment: Segment) = {
            val skipList =
              segment match {
                case segment: MemorySegment =>
                  Slice(segment.skipList)

                case segment: PersistentSegmentOne =>
                  Slice(segment.ref.skipList.get)

                case segment: PersistentSegmentMany =>
                  segment.segmentRefs.map(_.skipList.get)
              }

            skipList.size should be > 0

            skipList foreach {
              skipList =>
                skipList.asScala foreach {
                  case (key, value: KeyValue) =>
                    key.shouldBeSliced()
                    assertSliced(value)
                }
            }
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

          def doAssert(keyValues: Slice[Memory]) = {

            //read key-values so they are all part of the same byte array.
            val readKeyValues = writeAndRead(keyValues).get.map(_.toMemory)

            //assert that readKeyValues keys are not sliced.
            readKeyValues foreach assertNotSliced

            //Create Segment with sub-slice key-values and assert min & maxKey and also check that cached key-values are un-sliced.
            assertSegment(
              keyValues = readKeyValues,

              assert =
                (keyValues, segment) => {
                  assertMinAndMaxKeyAreSliced(segment)
                  //if Persistent Segment, read all key-values from disk so that they value added to cache.
                  if (persistent) assertGet(readKeyValues, segment)
                  //assert key-values added to cache are un-sliced
                  assertCacheKeyValuesAreSliced(segment)
                }
            )
          }

          runThis(100.times, log = true) {
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
                () => randomFixedKeyValue(nextData, nextData),
                () => randomFixedKeyValue(nextData, nextData),
                () => randomRangeKeyValue(nextData, nextData, randomFromValueOption(nextData), randomRangeValue(nextData))
              )

            doAssert(
              Random.shuffle(uninitialisedKeyValues ++ uninitialisedKeyValues).toSlice.map(_ ())
            )
          }
      }
    }

    "not create bloomFilter if the Segment has Remove range key-values or function key-values and set hasRange to true" in {
      TestCaseSweeper {
        implicit sweeper =>
          //adjustSegmentConfig = false so that Many Segments do not get created.

          def doAssert(keyValues: Slice[KeyValue], segment: Segment) = {
            segment.hasBloomFilter shouldBe false
            assertBloom(keyValues, segment)
            segment.hasRange.runRandomIO.right.value shouldBe true
            segment.close.runRandomIO.right.value
          }

          assertSegment(
            keyValues = Slice(Memory.put(0), Memory.Range(1, 10, FromValue.Null, Value.remove(randomDeadlineOption, Time.empty))),
            segmentConfig = SegmentBlock.Config.random.copy(Int.MaxValue, keyValuesCount),
            ensureOneSegmentOnly = false,
            assert = doAssert
          )

          assertSegment(
            keyValues = Slice(Memory.put(0), Memory.Range(1, 10, Value.remove(None, Time.empty), Value.remove(randomDeadlineOption, Time.empty))),
            segmentConfig = SegmentBlock.Config.random.copy(Int.MaxValue, keyValuesCount),
            ensureOneSegmentOnly = false,
            assert = doAssert
          )

          assertSegment(
            keyValues = Slice(Memory.put(0), Memory.Range(1, 10, Value.update(Slice.Null, randomDeadlineOption, Time.empty), Value.remove(randomDeadlineOption, Time.empty))),
            segmentConfig = SegmentBlock.Config.random.copy(Int.MaxValue, keyValuesCount),
            ensureOneSegmentOnly = false,
            assert = doAssert
          )

          assertSegment(
            keyValues = Slice(Memory.put(0), Memory.Range(1, 10, Value.put(1, randomDeadlineOption, Time.empty), Value.remove(randomDeadlineOption, Time.empty))),
            segmentConfig = SegmentBlock.Config.random.copy(Int.MaxValue, keyValuesCount),
            ensureOneSegmentOnly = false,
            assert = doAssert
          )
      }

      //      assertSegment(
      //        keyValues = Slice(Memory.put(0), Memory.Range(1, 10, Some(Value.PendingApply(Some(1), randomDeadlineOption, Time.empty)), Value.remove(randomDeadlineOption, Time.empty))),
      //        assert = doAssert
      //      )
    }

    "create bloomFilter if the Segment has no Remove range key-values but has update range key-values. And set hasRange to true" in {
      if (persistent) {
        TestCaseSweeper {
          implicit sweeper =>
            assertSegment(
              keyValues =
                Slice(Memory.put(0), Memory.put(1, 1), Memory.remove(2, randomDeadlineOption)),

              bloomFilterConfig =
                BloomFilterBlock.Config(
                  falsePositiveRate = 0.001,
                  minimumNumberOfKeys = 0,
                  optimalMaxProbe = optimalMaxProbe => optimalMaxProbe,
                  ioStrategy = _ => randomIOStrategy(),
                  compressions = _ => randomCompressionsOrEmpty()
                ),

              assert =
                (keyValues, segment) => {
                  segment.hasBloomFilter.runRandomIO.right.value shouldBe true
                  segment.hasRange.runRandomIO.right.value shouldBe false
                  segment.close.runRandomIO.right.value
                }
            )

            assertSegment(
              keyValues =
                Slice(Memory.put(0), Memory.Range(1, 10, FromValue.Null, Value.update(10, randomDeadlineOption))),

              bloomFilterConfig =
                BloomFilterBlock.Config(
                  falsePositiveRate = 0.001,
                  minimumNumberOfKeys = 0,
                  optimalMaxProbe = optimalMaxProbe => optimalMaxProbe,
                  ioStrategy = _ => randomIOStrategy(),
                  compressions = _ => randomCompressionsOrEmpty()
                ),

              assert =
                (keyValues, segment) => {
                  segment.hasBloomFilter.runRandomIO.right.value shouldBe true
                  segment.hasRange.runRandomIO.right.value shouldBe true
                  segment.close.runRandomIO.right.value
                }
            )
        }
      }
    }

    "set hasRange to true if the Segment contains Range key-values" in {
      TestCaseSweeper {
        implicit sweeper =>
          def doAssert(keyValues: Slice[KeyValue], segment: Segment): Unit = {
            segment.hasRange.runRandomIO.right.value shouldBe true
            segment.hasPut.runRandomIO.right.value shouldBe true
          }

          assertSegment(
            keyValues = Slice(Memory.put(0), Memory.Range(1, 10, FromValue.Null, Value.update(10))),
            assert = doAssert
          )

          assertSegment(
            keyValues = Slice(Memory.put(0), Memory.Range(1, 10, Value.remove(None, Time.empty), Value.update(10))),
            assert = doAssert
          )

          assertSegment(
            keyValues = Slice(Memory.put(0), Memory.Range(1, 10, Value.put(1), Value.update(10))),
            assert = doAssert
          )

          assertSegment(
            keyValues = Slice(Memory.put(0), Memory.Range(1, 10, FromValue.Null, Value.remove(None, Time.empty))),
            assert = doAssert
          )

          assertSegment(
            keyValues = Slice(Memory.put(0), Memory.Range(1, 10, Value.remove(10.seconds.fromNow), Value.remove(None, Time.empty))),
            assert = doAssert
          )

          assertSegment(
            keyValues = Slice(Memory.put(0), Memory.Range(1, 10, Value.put(1), Value.remove(None, Time.empty))),
            assert = doAssert
          )

          runThisParallel(100.times) {
            assertSegment(
              keyValues = Slice(Memory.put(0), randomRangeKeyValue(1, 10)),
              assert = doAssert
            )
          }

          assertSegment(
            keyValues = randomPutKeyValues(keyValuesCount, addRemoves = true, addRanges = true, addPutDeadlines = true, addRemoveDeadlines = true),
            assert = doAssert
          )
      }
    }

    "not overwrite a Segment if it already exists" in {
      if (memory) {
        //memory Segments do not check for overwrite. No tests required
      } else {
        TestCaseSweeper {
          implicit sweeper =>
            assertSegment(
              keyValues =
                randomPutKeyValues(keyValuesCount),

              assert =
                (keyValues, segment) =>
                  TestCaseSweeper {
                    implicit sweeper =>
                      val failedKV = randomKeyValues(keyValuesCount, addRemoves = true)
                      val reopenedSegment = IO(TestSegment(failedKV, path = segment.path))
                      reopenedSegment.left.right.value.exception shouldBe a[FileAlreadyExistsException]
                      //data remained unchanged
                      assertReads(keyValues, segment)
                      val readState = ThreadReadState.random
                      failedKV foreach {
                        keyValue =>
                          segment.get(keyValue.key, readState).runRandomIO.right.value.toOptional shouldBe empty
                      }
                      assertBloom(keyValues, segment)
                  }
            )
        }
      }
    }

    "initialise a segment that already exists" in {
      if (memory) {
        //memory Segments cannot re-initialise Segments after shutdown.
      } else {
        runThis(10.times) {
          TestCaseSweeper {
            implicit sweeper =>
              //set this MemorySweeper as root sweeper.
              TestSweeper.createKeyValueSweeperBlock().value.sweep()

              assertSegment(
                keyValues =
                  randomizedKeyValues(keyValuesCount),

                segmentConfig = SegmentBlock.Config.random(cacheBlocksOnCreate = false),

                closeAfterCreate =
                  true,

                testAgainAfterAssert =
                  false,

                assert =
                  (keyValues, segment) => {
                    segment.isOpen shouldBe false
                    segment.isFileDefined shouldBe false
                    segment.isKeyValueCacheEmpty shouldBe true

                    assertHigher(keyValues, segment)

                    segment.isOpen shouldBe true
                    segment.isFileDefined shouldBe true
                    segment.isKeyValueCacheEmpty shouldBe false

                    assertBloom(keyValues, segment)
                    segment.close.runRandomIO.right.value
                    segment.isOpen shouldBe false
                    segment.isFileDefined shouldBe false

                    segment.isKeyValueCacheEmpty shouldBe segment.isInstanceOf[PersistentSegmentMany]
                  }
              )
          }
        }
      }
    }

    "initialise a segment that already exists but Segment info is unknown" in {
      if (memory) {
        //memory Segments cannot re-initialise Segments after shutdown.
      } else {
        runThis(100.times, log = true) {
          TestCaseSweeper {
            implicit sweeper =>
              assertSegment(
                keyValues = randomizedKeyValues(keyValuesCount, startId = Some(0)),

                assert =
                  (keyValues, segment) => {
                    TestCaseSweeper {
                      implicit sweeper =>
                        import sweeper._

                        val readSegment =
                          Segment(
                            path = segment.path,
                            mmap = MMAP.randomForSegment(),
                            checkExists = randomBoolean()
                          )
                    }
                  }
              )
          }
        }
      }
    }

    "fail initialisation if the segment does not exist" in {
      if (memory) {
        //memory Segments do not value re-initialised
      } else {
        TestCaseSweeper {
          implicit sweeper =>
            import sweeper._

            //memory-mapped files on windows get submitted to ByteBufferCleaner.
            implicit val bufferCleaner = ByteBufferSweeper(messageReschedule = 0.seconds, actorStashCapacity = 0, actorInterval = 0.seconds).sweep()
            bufferCleaner.actor

            //create a segment and delete it
            val segment = TestSegment()
            segment.delete

            eventual(20.seconds) {
              IO(segment.asInstanceOf[PersistentSegment].tryReopen).left.right.value.exception shouldBe a[NoSuchFileException]
            }
        }
      }
    }
  }

  "deleteSegments" should {
    "delete multiple segments" in {
      runThis(100.times, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
            import sweeper._

            implicit val bufferCleaner = ByteBufferSweeper(messageReschedule = 0.seconds, actorStashCapacity = 0, actorInterval = 0.seconds).sweep()

            val segment1 = TestSegment(randomizedKeyValues(keyValuesCount))
            val segment2 = TestSegment(randomizedKeyValues(keyValuesCount))
            val segment3 = TestSegment(randomizedKeyValues(keyValuesCount))

            val deleted = Segment.deleteSegments(Seq(segment1, segment2, segment3))
            deleted shouldBe 3

            eventual(10.seconds) {
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
      }
    }
  }

  "Segment" should {
    "open a closed Segment on read and clear footer" in {
      runThis(10.times) {
        TestCaseSweeper {
          implicit sweeper =>
            //        implicit val fileSweeper = FileSweeper.Disabled
            implicit val blockCache: Option[BlockCache.State] = None
            implicit val fileSweeper = FileSweeper(50, ActorConfig.TimeLoop("", 10.seconds, TestExecutionContext.executionContext)).sweep().actor

            val keyValues = randomizedKeyValues(keyValuesCount)
            val segment = TestSegment(keyValues)

            def close: Unit = {
              segment.close
              if (levelStorage.persistent) {
                //also clear the cache so that if the key-value is a group on open file is still reopened
                //instead of just reading from in-memory Group key-value.
                eitherOne(segment.clearCachedKeyValues(), segment.clearAllCaches())
                segment.isFileDefined shouldBe false
                segment.isOpen shouldBe false
              }
            }

            def open(keyValue: KeyValue): Unit = {
              segment.get(keyValue.key, ThreadReadState.random).runRandomIO.value.getUnsafe shouldBe keyValue
              segment.isFileDefined shouldBe true
              segment.isOpen shouldBe true
            }

            keyValues foreach {
              keyValue =>
                close
                open(keyValue)
            }
        }
      }
    }

    "fail read and write operations on a Segment that does not exists" in {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._

          implicit val bufferCleaner = ByteBufferSweeper(messageReschedule = 0.seconds, actorStashCapacity = 0, actorInterval = 0.seconds).sweep()
          bufferCleaner.actor

          val keyValues = randomizedKeyValues(keyValuesCount)

          val valuesConfig: ValuesBlock.Config = ValuesBlock.Config.random
          val sortedIndexConfig: SortedIndexBlock.Config = SortedIndexBlock.Config.random
          val binarySearchIndexConfig: BinarySearchIndexBlock.Config = BinarySearchIndexBlock.Config.random
          val hashIndexConfig: HashIndexBlock.Config = HashIndexBlock.Config.random
          val bloomFilterConfig: BloomFilterBlock.Config = BloomFilterBlock.Config.random
          val segmentConfig: SegmentBlock.Config = SegmentBlock.Config.random

          val segment =
            TestSegment(
              keyValues = keyValues,
              valuesConfig = valuesConfig,
              sortedIndexConfig = sortedIndexConfig,
              binarySearchIndexConfig = binarySearchIndexConfig,
              hashIndexConfig = hashIndexConfig,
              bloomFilterConfig = bloomFilterConfig,
              segmentConfig = segmentConfig
            )

          segment.delete

          //segment eventually gets deleted
          eventual(5.seconds) {
            segment.isOpen shouldBe false
            segment.isFileDefined shouldBe false
            segment.existsOnDisk shouldBe false
          }

          IO(segment.get(keyValues.head.key, ThreadReadState.random)).left.get.exception shouldBe a[NoSuchFileException]

          IO {
            segment.put(
              newKeyValues = keyValues,
              removeDeletes = false,
              createdInLevel = 0,
              valuesConfig = valuesConfig,
              sortedIndexConfig = sortedIndexConfig,
              binarySearchIndexConfig = binarySearchIndexConfig,
              hashIndexConfig = hashIndexConfig,
              bloomFilterConfig = bloomFilterConfig,
              segmentConfig = segmentConfig
            )
          }.left.get.exception shouldBe a[NoSuchFileException]

          IO {
            segment.refresh(
              removeDeletes = false,
              createdInLevel = 0,
              valuesConfig = valuesConfig,
              sortedIndexConfig = sortedIndexConfig,
              binarySearchIndexConfig = binarySearchIndexConfig,
              hashIndexConfig = hashIndexConfig,
              bloomFilterConfig = bloomFilterConfig,
              segmentConfig = segmentConfig
            )
          }.left.get.exception shouldBe a[NoSuchFileException]

          segment.isOpen shouldBe false
          segment.isFileDefined shouldBe false
      }
    }

    "reopen closed channel for read when closed by LimitQueue" in {
      if (memory) {
        //memory Segments do not value closed via
      } else {
        runThis(5.times, log = true) {
          TestCaseSweeper {
            implicit sweeper =>
              implicit val fileSweeper = FileSweeper(1, ActorConfig.TimeLoop("", 2.second, ec)).actor.sweep()

              val keyValues = randomizedKeyValues(keyValuesCount)

              val segmentConfig = SegmentBlock.Config.random(cacheBlocksOnCreate = true, mmap = MMAP.Disabled, cacheOnAccess = true)

              val segment1 =
                TestSegment(
                  keyValues = keyValues,
                  segmentConfig = segmentConfig
                )

              segment1.isOpen shouldBe !segmentConfig.cacheBlocksOnCreate
              segment1.getKeyValueCount() shouldBe keyValues.size
              segment1.isOpen shouldBe !segmentConfig.cacheBlocksOnCreate

              //create another segment should close segment 1
              val segment2 = TestSegment(keyValues)
              segment2.getKeyValueCount() shouldBe keyValues.size

              eventual(5.seconds) {
                //segment one is closed
                segment1.isOpen shouldBe false
              }

              eventual(5.second) {
                //when it's close clear all the caches so that key-values do not get read from the cache.
                eitherOne(segment1.clearAllCaches(), segment1.clearCachedKeyValues())
                //read one key value from Segment1 so that it's reopened and added to the cache. This will also remove Segment 2 from cache
                segment1.get(keyValues.head.key, ThreadReadState.random).getUnsafe shouldBe keyValues.head
                segment1.isOpen shouldBe true
              }

              eventual(5.seconds) {
                //segment2 is closed
                segment2.isOpen shouldBe false
              }
          }
        }
      }
    }
  }

  "delete" should {
    "close the channel and delete the file" in {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._

          //set this MemorySweeper as root sweeper.
          TestSweeper.createKeyValueSweeperBlock().value.sweep()

          ByteBufferSweeper(messageReschedule = 0.seconds, actorStashCapacity = 0, actorInterval = 0.seconds).sweep()

          val keyValues = randomizedKeyValues(keyValuesCount)
          val segment = TestSegment(keyValues)
          assertReads(keyValues, segment) //populate the cache

          segment.cachedKeyValueSize shouldBe keyValues.size

          segment.delete
          segment.cachedKeyValueSize shouldBe keyValues.size //cache is not cleared

          eventual(5.seconds) {
            if (persistent) {
              segment.isOpen shouldBe false
              segment.isFooterDefined shouldBe false //on delete in-memory footer is cleared
            }
            segment.existsOnDisk shouldBe false
          }
      }
    }
  }

  "copyTo" should {
    "copy the segment to a target path without deleting the original" in {
      if (persistent) {
        TestCaseSweeper {
          implicit sweeper =>

            val keyValues = randomizedKeyValues(keyValuesCount)
            val keyValuesReadOnly = keyValues

            val segment = TestSegment(keyValues).asInstanceOf[PersistentSegmentOne]
            val targetPath = createRandomIntDirectory.resolve(nextId + s".${Extension.Seg}")

            segment.copyTo(targetPath)
            segment.existsOnDisk shouldBe true

            val copiedSegment = segment.reopen(targetPath)
            copiedSegment.toSlice() shouldBe keyValuesReadOnly
            copiedSegment.path shouldBe targetPath

            //original segment should still exist
            segment.toSlice() shouldBe keyValuesReadOnly
        }
      }
    }
  }

  "copyToPersist" should {
    "copy the segment and persist it to disk" in {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._
          val valuesConfig: ValuesBlock.Config = ValuesBlock.Config.random
          val sortedIndexConfig: SortedIndexBlock.Config = SortedIndexBlock.Config.random
          val binarySearchIndexConfig: BinarySearchIndexBlock.Config = BinarySearchIndexBlock.Config.random
          val hashIndexConfig: HashIndexBlock.Config = HashIndexBlock.Config.random
          val bloomFilterConfig: BloomFilterBlock.Config = BloomFilterBlock.Config.random
          val segmentConfig: SegmentBlock.Config = SegmentBlock.Config.random

          val keyValues = randomizedKeyValues(keyValuesCount)

          val segment =
            TestSegment(
              keyValues = keyValues,
              valuesConfig = valuesConfig,
              sortedIndexConfig = sortedIndexConfig,
              binarySearchIndexConfig = binarySearchIndexConfig,
              hashIndexConfig = hashIndexConfig,
              bloomFilterConfig = bloomFilterConfig,
              segmentConfig = segmentConfig
            )

          val pathDistributor = createPathDistributor

          val segments =
            Segment.copyToPersist(
              segment = segment,
              createdInLevel = 0,
              pathsDistributor = pathDistributor,
              valuesConfig = valuesConfig,
              sortedIndexConfig = sortedIndexConfig,
              binarySearchIndexConfig = binarySearchIndexConfig,
              hashIndexConfig = hashIndexConfig,
              bloomFilterConfig = bloomFilterConfig,
              segmentConfig = segmentConfig.copy(minSize = Segment.segmentSizeForMerge(segment) / 10),
              removeDeletes = false
            )

          if (persistent)
            segments.size shouldBe 1
          else
            segments.size should be > 1

          segments.foreach(_.existsOnDisk shouldBe true)
          Segment.getAllKeyValues(segments) shouldBe keyValues
      }
    }

    "copy the segment and persist it to disk when remove deletes is true" in {
      runThis(10.times) {
        TestCaseSweeper {
          implicit sweeper =>
            import sweeper._

            val keyValues = randomizedKeyValues(keyValuesCount)
            val segment = TestSegment(keyValues, segmentConfig = SegmentBlock.Config.random(minSegmentSize = Int.MaxValue, maxKeyValuesPerSegment = Int.MaxValue))

            val valuesConfig: ValuesBlock.Config = ValuesBlock.Config.random
            val sortedIndexConfig: SortedIndexBlock.Config = SortedIndexBlock.Config.random
            val binarySearchIndexConfig: BinarySearchIndexBlock.Config = BinarySearchIndexBlock.Config.random
            val hashIndexConfig: HashIndexBlock.Config = HashIndexBlock.Config.random
            val bloomFilterConfig: BloomFilterBlock.Config = BloomFilterBlock.Config.random
            val segmentConfig: SegmentBlock.Config = SegmentBlock.Config.random.copy(minSize = Segment.segmentSizeForMerge(segment) / 10)

            val pathDistributor = createPathDistributor

            val segments =
              Segment.copyToPersist(
                segment = segment,
                createdInLevel = 0,
                pathsDistributor = pathDistributor,
                valuesConfig = valuesConfig,
                sortedIndexConfig = sortedIndexConfig,
                binarySearchIndexConfig = binarySearchIndexConfig,
                hashIndexConfig = hashIndexConfig,
                bloomFilterConfig = bloomFilterConfig,
                segmentConfig = segmentConfig,
                removeDeletes = true
              )

            segments.foreach(_.existsOnDisk shouldBe true)

            if (persistent)
              Segment.getAllKeyValues(segments) shouldBe keyValues //persistent Segments are simply copied and are not checked for removed key-values.
            else
              Segment.getAllKeyValues(segments) shouldBe keyValues.collect { //memory Segments does a split/merge and apply lastLevel rules.
                case keyValue: Memory.Put if keyValue.hasTimeLeft() =>
                  keyValue

                case Memory.Range(fromKey, _, put @ Value.Put(_, deadline, _), _) if deadline.forall(_.hasTimeLeft()) =>
                  put.toMemory(fromKey)
              }
        }
      }
    }

    "revert copy if Segment initialisation fails after copy" in {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._

          val keyValues = randomizedKeyValues(keyValuesCount)
          val segment = TestSegment(keyValues)

          val valuesConfig: ValuesBlock.Config = ValuesBlock.Config.random
          val sortedIndexConfig: SortedIndexBlock.Config = SortedIndexBlock.Config.random
          val binarySearchIndexConfig: BinarySearchIndexBlock.Config = BinarySearchIndexBlock.Config.random
          val hashIndexConfig: HashIndexBlock.Config = HashIndexBlock.Config.random
          val bloomFilterConfig: BloomFilterBlock.Config = BloomFilterBlock.Config.random
          val segmentConfig: SegmentBlock.Config = SegmentBlock.Config.random.copy(minSize = Segment.segmentSizeForMerge(segment) / 10)

          val pathDistributor = createPathDistributor

          val segmentId = idGenerator.nextID
          val conflictingPath = pathDistributor.next.resolve(IDGenerator.segmentId(segmentId))
          Effect.createFile(conflictingPath) //path already taken.

          implicit val segmentIDGenerator: IDGenerator = IDGenerator(segmentId - 1)

          assertThrows[FileAlreadyExistsException] {
            Segment.copyToPersist(
              segment = segment,
              createdInLevel = 0,
              pathsDistributor = pathDistributor,
              valuesConfig = valuesConfig,
              sortedIndexConfig = sortedIndexConfig,
              binarySearchIndexConfig = binarySearchIndexConfig,
              hashIndexConfig = hashIndexConfig,
              bloomFilterConfig = bloomFilterConfig,
              segmentConfig = segmentConfig,
              removeDeletes = true
            )(keyOrder = keyOrder,
              timeOrder = timeOrder,
              functionStore = functionStore,
              keyValueMemorySweeper = keyValueMemorySweeper,
              fileSweeper = fileSweeper,
              bufferCleaner = cleaner,
              blockCache = blockCache,
              segmentIO = segmentIO,
              idGenerator = segmentIDGenerator
            )
          }

          //windows
          if (isWindowsAndMMAPSegments())
            sweeper.receiveAll()

          Effect.size(conflictingPath) shouldBe 0
          if (persistent) segment.existsOnDisk shouldBe true //original Segment remains untouched
      }
    }

    "revert copy of Key-values if creating at least one Segment fails" in {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._

          val keyValues = randomizedKeyValues(keyValuesCount)

          val valuesConfig: ValuesBlock.Config = ValuesBlock.Config.random
          val sortedIndexConfig: SortedIndexBlock.Config = SortedIndexBlock.Config.random
          val binarySearchIndexConfig: BinarySearchIndexBlock.Config = BinarySearchIndexBlock.Config.random
          val hashIndexConfig: HashIndexBlock.Config = HashIndexBlock.Config.random
          val bloomFilterConfig: BloomFilterBlock.Config = BloomFilterBlock.Config.random
          val segmentConfig: SegmentBlock.Config = SegmentBlock.Config.random

          //used to calculate the size of Segment
          val temporarySegment =
            TestSegment(
              keyValues = keyValues,
              valuesConfig = valuesConfig,
              sortedIndexConfig = sortedIndexConfig,
              binarySearchIndexConfig = binarySearchIndexConfig,
              hashIndexConfig = hashIndexConfig,
              bloomFilterConfig = bloomFilterConfig,
              segmentConfig = segmentConfig
            )

          val pathDistributor = createPathDistributor

          val segmentId = idGenerator.nextID

          Effect.createFile(pathDistributor.next.resolve(IDGenerator.segmentId(segmentId + 4))) //path already taken.

          levelStorage.dirs foreach {
            dir =>
              Effect.createDirectoriesIfAbsent(dir.path)
              IO(Effect.createFile(dir.path.resolve(IDGenerator.segmentId(segmentId + 4)))) //path already taken.
          }

          val filesBeforeCopy = pathDistributor.next.files(Extension.Seg)
          filesBeforeCopy.size shouldBe 1

          assertThrows[FileAlreadyExistsException] {
            Segment.copyToPersist(
              keyValues = keyValues,
              createdInLevel = 0,
              pathsDistributor = pathDistributor,
              valuesConfig = valuesConfig,
              sortedIndexConfig = sortedIndexConfig,
              binarySearchIndexConfig = binarySearchIndexConfig,
              hashIndexConfig = hashIndexConfig,
              bloomFilterConfig = bloomFilterConfig,
              segmentConfig = segmentConfig.copy(minSize = Segment.segmentSizeForMerge(temporarySegment) / 20),
              removeDeletes = false
            )
          }

          //process all delete requests
          if (OperatingSystem.isWindows)
            eventual(1.minute) {
              sweeper.receiveAll()
              pathDistributor.next.files(Extension.Seg) shouldBe filesBeforeCopy
            }
          else
            pathDistributor.next.files(Extension.Seg) shouldBe filesBeforeCopy

      }
    }
  }

  "copyToMemory" should {
    "copy persistent segment and store it in Memory" in {
      runThis(100.times) {
        TestCaseSweeper {
          implicit sweeper =>
            import sweeper._
            val keyValues = randomizedKeyValues(keyValuesCount)
            val segment = TestSegment(keyValues, segmentConfig = SegmentBlock.Config.random(minSegmentSize = Int.MaxValue, maxKeyValuesPerSegment = Int.MaxValue))

            val memorySize = keyValues.foldLeft(0)(_ + MergeStats.Memory.calculateSize(_))

            val pathDistributor = createPathDistributor

            val segments =
              Segment.copyToMemory(
                segment = segment,
                pathsDistributor = pathDistributor,
                createdInLevel = 0,
                removeDeletes = false,
                minSegmentSize =
                  //there are too many conditions that will not split the segments so set the size of each segment to be too small
                  //for the split to occur.
                  memorySize / 10,
                maxKeyValueCountPerSegment = randomIntMax(keyValues.size)
              )

            segments.size should be >= 2 //ensures that splits occurs. Memory Segments do not value written to disk without splitting.

            segments.foreach(_.existsOnDisk shouldBe false)
            Segment.getAllKeyValues(segments) shouldBe keyValues
        }
      }
    }

    "copy the segment and persist it to disk when removeDeletes is true" in {
      runThis(10.times) {
        TestCaseSweeper {
          implicit sweeper =>
            import sweeper._
            val keyValues = randomizedKeyValues(keyValuesCount)
            val segment = TestSegment(keyValues)

            val pathDistributor = createPathDistributor

            val memorySize = keyValues.foldLeft(0)(_ + MergeStats.Memory.calculateSize(_))

            val segments =
              Segment.copyToMemory(
                segment = segment,
                createdInLevel = 0,
                pathsDistributor = pathDistributor,
                removeDeletes = true,
                minSegmentSize = memorySize / 1000,
                maxKeyValueCountPerSegment = randomIntMax(keyValues.size)
              )

            segments.foreach(_.existsOnDisk shouldBe false)

            segments.size should be >= 2 //ensures that splits occurs. Memory Segments do not value written to disk without splitting.

            //some key-values could value expired while unexpired key-values are being collected. So try again!
            Segment.getAllKeyValues(segments) shouldBe keyValues.collect {
              case keyValue: Memory.Put if keyValue.hasTimeLeft() =>
                keyValue
              case Memory.Range(fromKey, _, put @ Value.Put(_, deadline, _), _) if deadline.forall(_.hasTimeLeft()) =>
                put.toMemory(fromKey)
            }
        }
      }
    }
  }

  "put" should {
    "return None for empty byte arrays for values" in {
      TestCaseSweeper {
        implicit sweeper =>

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
                    left = randomFixedKeyValue(nextKey, Slice.empty),
                    mid = randomRangeKeyValue(nextKey, nextKey, randomFromValueOption(Slice.emptyBytes), randomRangeValue(Slice.emptyBytes)),
                    right = randomFixedKeyValue(nextKey, Slice.empty)
                  )
                key
            }

            val segment = TestSegment(keyValuesWithEmptyValues.toSlice, segmentConfig = SegmentBlock.Config.random(minSegmentSize = Int.MaxValue, maxKeyValuesPerSegment = Int.MaxValue))

            def valuesValueShouldBeNone(value: Value): Unit =
              value match {
                case Value.Update(value, deadline, time) =>
                  value shouldBe Slice.Null
                case Value.Put(value, deadline, time) =>
                  value shouldBe Slice.Null
                case Value.PendingApply(applies) =>
                  applies foreach valuesValueShouldBeNone
                case Value.Function(function, time) =>
                //nothing to assert
                case Value.Remove(deadline, time) =>
                //nothing to assert
              }

            segment.toSlice() foreach {
              case keyValue: KeyValue.Put =>
                keyValue.getOrFetchValue shouldBe Slice.Null

              case keyValue: KeyValue.Update =>
                keyValue.getOrFetchValue shouldBe Slice.Null

              case keyValue: KeyValue.Range =>
                val (fromValue, rangeValue) = keyValue.fetchFromAndRangeValueUnsafe
                Seq(fromValue.toOptionS, Some(rangeValue)).flatten foreach valuesValueShouldBeNone

              case apply: KeyValue.PendingApply =>
                apply.getOrFetchApplies foreach valuesValueShouldBeNone

              case _: KeyValue.Function =>
              //nothing to assert
              case _: KeyValue.Remove =>
              //nothing to assert
            }
          }
      }
    }

    "reopen closed channel" in {
      TestCaseSweeper {
        implicit sweeper =>

          val keyValues1 = randomizedKeyValues(keyValuesCount)

          val segment = TestSegment(keyValues1)
          segment.close
          if (persistent) segment.isOpen shouldBe false

          val keyValues2 = randomizedKeyValues(keyValuesCount)

          segment.put(
            newKeyValues = keyValues2,
            valuesConfig = ValuesBlock.Config.random,
            sortedIndexConfig = SortedIndexBlock.Config.random,
            binarySearchIndexConfig = BinarySearchIndexBlock.Config.random,
            hashIndexConfig = HashIndexBlock.Config.random,
            bloomFilterConfig = BloomFilterBlock.Config.random,
            segmentConfig = SegmentBlock.Config.random.copy(minSize = 1.mb),
            removeDeletes = false,
            createdInLevel = 0
          )

          if (persistent) segment.isOpen shouldBe true
      }
    }

    "return a new segment with merged key values" in {
      TestCaseSweeper {
        implicit sweeper =>

          val keyValues = Slice(Memory.put(1, 1))

          val valuesConfig: ValuesBlock.Config = ValuesBlock.Config.random
          val sortedIndexConfig: SortedIndexBlock.Config = SortedIndexBlock.Config.random
          val binarySearchIndexConfig: BinarySearchIndexBlock.Config = BinarySearchIndexBlock.Config.random
          val hashIndexConfig: HashIndexBlock.Config = HashIndexBlock.Config.random
          val bloomFilterConfig: BloomFilterBlock.Config = BloomFilterBlock.Config.random
          val segmentConfig: SegmentBlock.Config = SegmentBlock.Config.random

          val segment =
            TestSegment(
              keyValues = keyValues,
              valuesConfig = valuesConfig,
              sortedIndexConfig = sortedIndexConfig,
              binarySearchIndexConfig = binarySearchIndexConfig,
              hashIndexConfig = hashIndexConfig,
              bloomFilterConfig = bloomFilterConfig,
              segmentConfig = segmentConfig
            )

          val newKeyValues = Slice(Memory.put(2, 2))

          val newSegments =
            segment.put(
              newKeyValues = newKeyValues,
              removeDeletes = false,
              createdInLevel = 0,
              valuesConfig = valuesConfig,
              sortedIndexConfig = sortedIndexConfig,
              binarySearchIndexConfig = binarySearchIndexConfig,
              hashIndexConfig = hashIndexConfig,
              bloomFilterConfig = bloomFilterConfig,
              segmentConfig = segmentConfig.copy(minSize = 4.mb)
            )

          newSegments should have size 1

          val allReadKeyValues = Segment.getAllKeyValues(newSegments)

          allReadKeyValues should have size 2

          val builder = MergeStats.random()

          SegmentMerger.merge(
            newKeyValues = newKeyValues,
            oldKeyValuesCount = keyValues.size,
            oldKeyValues = keyValues.iterator,
            stats = builder,
            isLastLevel = false
          )

          val expectedKeyValues = builder.result

          expectedKeyValues.result should have size 2

          allReadKeyValues shouldBe expectedKeyValues.result
      }
    }

    "return multiple new segments with merged key values" in {
      TestCaseSweeper {
        implicit sweeper =>

          val keyValues = randomizedKeyValues(keyValuesCount)
          val segment = TestSegment(keyValues)

          val newKeyValues = randomizedKeyValues(keyValuesCount)

          val valuesConfig: ValuesBlock.Config = ValuesBlock.Config.random
          val sortedIndexConfig: SortedIndexBlock.Config = SortedIndexBlock.Config.random
          val binarySearchIndexConfig: BinarySearchIndexBlock.Config = BinarySearchIndexBlock.Config.random
          val hashIndexConfig: HashIndexBlock.Config = HashIndexBlock.Config.random
          val bloomFilterConfig: BloomFilterBlock.Config = BloomFilterBlock.Config.random
          val segmentConfig: SegmentBlock.Config = SegmentBlock.Config.random

          val newSegments =
            segment.put(
              newKeyValues = newKeyValues,
              valuesConfig = valuesConfig,
              sortedIndexConfig = sortedIndexConfig,
              binarySearchIndexConfig = binarySearchIndexConfig,
              hashIndexConfig = hashIndexConfig,
              bloomFilterConfig = bloomFilterConfig,
              segmentConfig = segmentConfig.copy(minSize = segment.segmentSize / 10),
              removeDeletes = false,
              createdInLevel = 0
            )

          newSegments.size should be > 1

          val allReadKeyValues = Segment.getAllKeyValues(newSegments)

          val builder = MergeStats.random()

          //give merge a very large size so that there are no splits (test convenience)
          SegmentMerger.merge(
            newKeyValues = newKeyValues,
            oldKeyValues = keyValues,
            stats = builder,
            isLastLevel = false
          )

          //allReadKeyValues are read from multiple Segments so valueOffsets will be invalid so stats will be invalid
          allReadKeyValues shouldBe builder.result
      }
    }

    "fail put and delete partially written batch Segments if there was a failure in creating one of them" in {
      if (memory) {
        // not for in-memory Segments
      } else {
        TestCaseSweeper {
          implicit sweeper =>

            val valuesConfig: ValuesBlock.Config = ValuesBlock.Config.random
            val sortedIndexConfig: SortedIndexBlock.Config = SortedIndexBlock.Config.random
            val binarySearchIndexConfig: BinarySearchIndexBlock.Config = BinarySearchIndexBlock.Config.random
            val hashIndexConfig: HashIndexBlock.Config = HashIndexBlock.Config.random
            val bloomFilterConfig: BloomFilterBlock.Config = BloomFilterBlock.Config.random
            val segmentConfig: SegmentBlock.Config = SegmentBlock.Config.random

            val keyValues = randomizedKeyValues(keyValuesCount)
            val segment = TestSegment(keyValues)
            val newKeyValues = randomizedKeyValues(keyValuesCount)

            val tenthSegmentId = {
              val segmentId = (segment.path.fileId._1 + 10).toSegmentFileId
              segment.path.getParent.resolve(segmentId)
            }

            //create a segment with the next id in sequence which should fail put with FileAlreadyExistsException
            val segmentToFailPut = TestSegment(path = tenthSegmentId)

            assertThrows[FileAlreadyExistsException] {
              segment.put(
                newKeyValues = newKeyValues,
                removeDeletes = false,
                createdInLevel = 0,
                valuesConfig = valuesConfig,
                sortedIndexConfig = sortedIndexConfig,
                binarySearchIndexConfig = binarySearchIndexConfig,
                hashIndexConfig = hashIndexConfig,
                bloomFilterConfig = bloomFilterConfig,
                segmentConfig = segmentConfig.copy(minSize = 50.bytes)
              )
            }

            //the folder should contain only the original segment and the segmentToFailPut
            def assertFinalFiles() = segment.path.getParent.files(Extension.Seg) should contain only(segment.path, segmentToFailPut.path)

            //if windows execute all stashed actor messages
            if (OperatingSystem.isWindows) {
              eventual(1.minute) {
                sweeper.receiveAll()
                assertFinalFiles()
              }
            } else {
              assertFinalFiles()
            }
        }
      }
    }

    "return new segment with deleted KeyValues if all keys were deleted and removeDeletes is false" in {
      TestCaseSweeper {
        implicit sweeper =>

          implicit def testTimer: TestTimer = TestTimer.Empty

          val keyValues = Slice(
            Memory.put(1),
            Memory.put(2),
            Memory.put(3),
            Memory.put(4),
            Memory.Range(5, 10, FromValue.Null, Value.Update(Slice.Null, None, testTimer.next))
          )
          val segment = TestSegment(keyValues)
          assertGet(keyValues, segment)

          val deleteKeyValues = Slice(Memory.remove(1), Memory.remove(2), Memory.remove(3), Memory.remove(4), Memory.Range(5, 10, FromValue.Null, Value.remove(None)))

          val valuesConfig: ValuesBlock.Config = ValuesBlock.Config.random
          val sortedIndexConfig: SortedIndexBlock.Config = SortedIndexBlock.Config.random
          val binarySearchIndexConfig: BinarySearchIndexBlock.Config = BinarySearchIndexBlock.Config.random
          val hashIndexConfig: HashIndexBlock.Config = HashIndexBlock.Config.random
          val bloomFilterConfig: BloomFilterBlock.Config = BloomFilterBlock.Config.random
          val segmentConfig: SegmentBlock.Config = SegmentBlock.Config.random

          val deletedSegment =
            segment.put(
              newKeyValues = deleteKeyValues,
              removeDeletes = false,
              createdInLevel = 0,
              valuesConfig = valuesConfig,
              sortedIndexConfig = sortedIndexConfig,
              binarySearchIndexConfig = binarySearchIndexConfig,
              hashIndexConfig = hashIndexConfig,
              bloomFilterConfig = bloomFilterConfig,
              segmentConfig = segmentConfig.copy(minSize = 4.mb)
            )

          deletedSegment should have size 1
          val newDeletedSegment = deletedSegment.head
          newDeletedSegment.toSlice() shouldBe deleteKeyValues

          assertGet(keyValues, segment)
          if (persistent) assertGet(keyValues, segment.asInstanceOf[PersistentSegment].reopen)
      }
    }

    "return new segment with updated KeyValues if all keys values were updated to None" in {
      TestCaseSweeper {
        implicit sweeper =>

          implicit val testTimer: TestTimer = TestTimer.Incremental()

          val keyValues = randomizedKeyValues(count = keyValuesCount)
          val segment = TestSegment(keyValues)

          val updatedKeyValues = Slice.create[Memory](keyValues.size)
          keyValues.foreach(keyValue => updatedKeyValues add Memory.put(keyValue.key, Slice.Null))

          val valuesConfig: ValuesBlock.Config = ValuesBlock.Config.random
          val sortedIndexConfig: SortedIndexBlock.Config = SortedIndexBlock.Config.random
          val binarySearchIndexConfig: BinarySearchIndexBlock.Config = BinarySearchIndexBlock.Config.random
          val hashIndexConfig: HashIndexBlock.Config = HashIndexBlock.Config.random
          val bloomFilterConfig: BloomFilterBlock.Config = BloomFilterBlock.Config.random
          val segmentConfig: SegmentBlock.Config = SegmentBlock.Config.random

          val updatedSegments =
            segment.put(
              newKeyValues = updatedKeyValues,
              removeDeletes = true,
              createdInLevel = 0,
              valuesConfig = valuesConfig,
              sortedIndexConfig = sortedIndexConfig,
              binarySearchIndexConfig = binarySearchIndexConfig,
              hashIndexConfig = hashIndexConfig,
              bloomFilterConfig = bloomFilterConfig,
              segmentConfig = segmentConfig.copy(minSize = 4.mb)
            )

          updatedSegments should have size 1

          val newUpdatedSegment = updatedSegments.head
          newUpdatedSegment.toSlice() shouldBe updatedKeyValues

          assertGet(updatedKeyValues, newUpdatedSegment)
      }
    }

    "merge existing segment file with new KeyValues returning new segment file with updated KeyValues" in {
      runThis(10.times) {
        TestCaseSweeper {
          implicit sweeper =>

            implicit val testTimer: TestTimer = TestTimer.Incremental()
            //ranges value split to make sure there are no ranges.
            val keyValues1 = randomizedKeyValues(count = keyValuesCount, addRanges = false)
            val segment1 = TestSegment(keyValues1)

            val keyValues2Unclosed = Slice.create[Memory](keyValues1.size * 100)
            keyValues1 foreach {
              keyValue =>
                keyValues2Unclosed add randomPutKeyValue(keyValue.key)
            }

            val keyValues2Closed = keyValues2Unclosed.close()

            val segmentConfig2 = SegmentBlock.Config.random.copy(Int.MaxValue, if (memory) keyValues2Closed.size else randomIntMax(keyValues2Closed.size))

            val segment2 = TestSegment(keyValues2Closed, segmentConfig = segmentConfig2)

            val valuesConfig: ValuesBlock.Config = ValuesBlock.Config.random
            val sortedIndexConfig: SortedIndexBlock.Config = SortedIndexBlock.Config.random
            val binarySearchIndexConfig: BinarySearchIndexBlock.Config = BinarySearchIndexBlock.Config.random
            val hashIndexConfig: HashIndexBlock.Config = HashIndexBlock.Config.random
            val bloomFilterConfig: BloomFilterBlock.Config = BloomFilterBlock.Config.random
            val segmentConfig: SegmentBlock.Config = SegmentBlock.Config.random

            val mergedSegments =
              segment1.put(
                newKeyValues = segment2.toSlice(),
                removeDeletes = false,
                createdInLevel = 0,
                valuesConfig = valuesConfig,
                sortedIndexConfig = sortedIndexConfig,
                binarySearchIndexConfig = binarySearchIndexConfig,
                hashIndexConfig = hashIndexConfig,
                bloomFilterConfig = bloomFilterConfig,
                segmentConfig = segmentConfig.copy(minSize = 10.mb)
              )

            mergedSegments.size shouldBe 1
            val mergedSegment = mergedSegments.head

            //test merged segment should contain all
            val readState = ThreadReadState.random
            keyValues2Closed foreach {
              keyValue =>
                mergedSegment.get(keyValue.key, readState).getUnsafe shouldBe keyValue
            }

            mergedSegment.toSlice().size shouldBe keyValues2Closed.size
        }
      }
    }

    "return no new segments if all the KeyValues in the Segment were deleted and if remove deletes is true" in {
      runThis(50.times) {
        TestCaseSweeper {
          implicit sweeper =>

            val keyValues =
              Slice(
                randomFixedKeyValue(1),
                randomFixedKeyValue(2),
                randomFixedKeyValue(3),
                randomFixedKeyValue(4),
                randomRangeKeyValue(5, 10, randomRangeValue(), randomRangeValue())
              )

            val segment = TestSegment(keyValues)

            val deleteKeyValues = Slice.create[Memory](keyValues.size)
            (1 to 4).foreach(key => deleteKeyValues add Memory.remove(key))
            deleteKeyValues add Memory.Range(5, 10, FromValue.Null, Value.remove(None))

            segment.put(
              newKeyValues = deleteKeyValues,
              removeDeletes = true,
              createdInLevel = 0,
              valuesConfig = ValuesBlock.Config.random,
              sortedIndexConfig = SortedIndexBlock.Config.random,
              binarySearchIndexConfig = BinarySearchIndexBlock.Config.random,
              hashIndexConfig = HashIndexBlock.Config.random,
              bloomFilterConfig = BloomFilterBlock.Config.random,
              segmentConfig = SegmentBlock.Config.random.copy(minSize = 4.mb)
            ) shouldBe empty
        }
      }
    }

    "slice Put range into slice with fromValue set to Remove" in {
      TestCaseSweeper {
        implicit sweeper =>

          implicit val testTimer: TestTimer = TestTimer.Empty

          val keyValues = Slice(Memory.Range(1, 10, FromValue.Null, Value.update(10)))
          val segment = TestSegment(keyValues)

          val deleteKeyValues = Slice.create[Memory](10)
          (1 to 10).foreach(key => deleteKeyValues add Memory.remove(key))

          val removedRanges =
            segment.put(
              newKeyValues = deleteKeyValues,
              removeDeletes = false,
              createdInLevel = 0,
              valuesConfig = ValuesBlock.Config.random,
              sortedIndexConfig = SortedIndexBlock.Config.random,
              binarySearchIndexConfig = BinarySearchIndexBlock.Config.random,
              hashIndexConfig = HashIndexBlock.Config.random,
              bloomFilterConfig = BloomFilterBlock.Config.random,
              segmentConfig = SegmentBlock.Config.random.copy(minSize = 4.mb)
            ).head.toSlice()

          val expected: Seq[Memory] = (1 to 9).map(key => Memory.Range(key, key + 1, Value.remove(None), Value.update(10))) :+ Memory.remove(10)

          removedRanges shouldBe expected
      }
    }

    "return 1 new segment with only 1 key-value if all the KeyValues in the Segment were deleted but 1" in {
      TestCaseSweeper {
        implicit sweeper =>

          implicit val testTimer: TestTimer = TestTimer.Empty

          val keyValues = randomKeyValues(count = keyValuesCount)
          val segment = TestSegment(keyValues)

          val deleteKeyValues = Slice.create[Memory.Remove](keyValues.size - 1)
          keyValues.drop(1).foreach(keyValue => deleteKeyValues add Memory.remove(keyValue.key))

          val newSegments =
            segment.put(
              newKeyValues = deleteKeyValues,
              removeDeletes = true,
              createdInLevel = 0,
              valuesConfig = ValuesBlock.Config.random,
              sortedIndexConfig = SortedIndexBlock.Config.random,
              binarySearchIndexConfig = BinarySearchIndexBlock.Config.random,
              hashIndexConfig = HashIndexBlock.Config.random,
              bloomFilterConfig = BloomFilterBlock.Config.random,
              segmentConfig = SegmentBlock.Config.random.copy(minSize = 4.mb)
            )

          newSegments.size shouldBe 1
          newSegments.head.getKeyValueCount() shouldBe 1

          val newSegment = newSegments.head
          val keyValue = keyValues.head

          newSegment.get(keyValue.key, ThreadReadState.random).getUnsafe shouldBe keyValue

          newSegment.lower(keyValue.key, ThreadReadState.random).toOptional shouldBe empty
          newSegment.higher(keyValue.key, ThreadReadState.random).toOptional shouldBe empty
      }
    }

    "distribute new Segments to multiple folders equally" in {
      TestCaseSweeper {
        implicit sweeper =>

          val keyValues1 = Slice(Memory.put(1, 1), Memory.put(2, 2), Memory.put(3, 3), Memory.put(4, 4), Memory.put(5, 5), Memory.put(6, 6))
          val segment = TestSegment(keyValues1)

          val keyValues2 = Slice(Memory.put(7, 7), Memory.put(8, 8), Memory.put(9, 9), Memory.put(10, 10), Memory.put(11, 11), Memory.put(12, 12))

          val dirs = (1 to 6) map (_ => Dir(createRandomIntDirectory, 1))

          val segmentSizeForMerge = Segment.segmentSizeForMerge(segment)

          val pathsDistributor = PathsDistributor(dirs, () => Seq(segment))
          val segments =
            if (persistent)
              segment.put(
                newKeyValues = keyValues2,
                removeDeletes = false,
                createdInLevel = 0,
                valuesConfig = ValuesBlock.Config.random,
                sortedIndexConfig = SortedIndexBlock.Config.random,
                binarySearchIndexConfig = BinarySearchIndexBlock.Config.random,
                hashIndexConfig = HashIndexBlock.Config.random,
                bloomFilterConfig = BloomFilterBlock.Config.random,
                segmentConfig = SegmentBlock.Config.random.copy(minSize = segmentSizeForMerge / 4),
                pathsDistributor = pathsDistributor
              )
            else
              segment.put(
                newKeyValues = keyValues2,
                removeDeletes = false,
                createdInLevel = 0,
                valuesConfig = ValuesBlock.Config.random,
                sortedIndexConfig = SortedIndexBlock.Config.random,
                binarySearchIndexConfig = BinarySearchIndexBlock.Config.random,
                hashIndexConfig = HashIndexBlock.Config.random,
                bloomFilterConfig = BloomFilterBlock.Config.random,
                segmentConfig = SegmentBlock.Config.random.copy(minSize = 21.bytes),
                pathsDistributor = pathsDistributor
              )

          //all returned segments contain all the KeyValues ???
          //      segments should have size 5
          //      segments(0).getAll().value shouldBe keyValues1.slice(0, 1).unslice()
          //      segments(1).getAll().value shouldBe keyValues1.slice(2, 3).unslice()
          //      segments(2).getAll().value shouldBe keyValues1.slice(4, 5).unslice()
          //      segments(3).getAll().value shouldBe keyValues2.slice(0, 1).unslice()
          //      segments(4).getAll().value shouldBe keyValues2.slice(2, 3).unslice()
          //      segments(5).getAll().value shouldBe keyValues2.slice(4, 5).unslice()

          //all the paths are used to write Segments
          segments(0).path.getParent shouldBe dirs(0).path
          segments(1).path.getParent shouldBe dirs(1).path
          segments(2).path.getParent shouldBe dirs(2).path
          segments(3).path.getParent shouldBe dirs(3).path

          if (persistent)
            segments(4).path.getParent shouldBe dirs(4).path

        //all paths are used ???
        //      distributor.queuedPaths shouldBe empty
      }
    }
  }

  "refresh" should {
    "return new Segment with Removed key-values removed" in {
      if (persistent) {
        TestCaseSweeper {
          implicit sweeper =>

            val keyValues =
              (1 to 100) map {
                key =>
                  eitherOne(randomRemoveKeyValue(key), randomRangeKeyValue(key, key + 1, FromValue.Null, randomRangeValue()))
              } toSlice

            val segment = TestSegment(keyValues).asInstanceOf[PersistentSegment]
            segment.getKeyValueCount() shouldBe keyValues.size
            segment.toSlice() shouldBe keyValues

            val reopened = segment.reopen(segment.path)
            reopened.getKeyValueCount() shouldBe keyValues.size
            reopened.refresh(
              removeDeletes = true,
              createdInLevel = 0,
              valuesConfig = ValuesBlock.Config.random,
              sortedIndexConfig = SortedIndexBlock.Config.random,
              binarySearchIndexConfig = BinarySearchIndexBlock.Config.random,
              hashIndexConfig = HashIndexBlock.Config.random,
              bloomFilterConfig = BloomFilterBlock.Config.random,
              segmentConfig = SegmentBlock.Config.random
            ) shouldBe empty
        }
      }
    }

    "return no new Segments if all the key-values in the Segment were expired" in {
      TestCaseSweeper {
        implicit sweeper =>

          val keyValues1 = (1 to 100).map(key => randomPutKeyValue(key, deadline = Some(1.second.fromNow))).toSlice
          val segment = TestSegment(keyValues1)
          segment.getKeyValueCount() shouldBe keyValues1.size

          sleep(2.seconds)

          segment.refresh(
            removeDeletes = true,
            createdInLevel = 0,
            valuesConfig = ValuesBlock.Config.random,
            sortedIndexConfig = SortedIndexBlock.Config.random,
            binarySearchIndexConfig = BinarySearchIndexBlock.Config.random,
            hashIndexConfig = HashIndexBlock.Config.random,
            bloomFilterConfig = BloomFilterBlock.Config.random,
            segmentConfig = SegmentBlock.Config.random
          ) shouldBe empty
      }
    }

    "return all key-values when removeDeletes is false and when Segment was created in another Level" in {
      runThis(5.times) {
        TestCaseSweeper {
          implicit sweeper =>

            val keyValues: Slice[Memory.Put] = (1 to 100).map(key => Memory.put(key, key, 1.second)).toSlice

            val segment =
              Benchmark(s"Created Segment: ${keyValues.size} keyValues", inlinePrint = true) {
                TestSegment(
                  keyValues = keyValues,
                  createdInLevel = 3
                )
              }

            segment.getKeyValueCount() shouldBe keyValues.size

            sleep(2.seconds)

            val refresh =
              Benchmark(s"Refresh Segment: ${keyValues.size} keyValues", inlinePrint = true) {
                segment.refresh(
                  removeDeletes = false,
                  createdInLevel = 5,
                  valuesConfig = ValuesBlock.Config.random,
                  sortedIndexConfig = SortedIndexBlock.Config.random,
                  binarySearchIndexConfig = BinarySearchIndexBlock.Config.random,
                  hashIndexConfig = HashIndexBlock.Config.random,
                  bloomFilterConfig = BloomFilterBlock.Config.random,
                  segmentConfig = SegmentBlock.Config.random
                )
              }

            refresh should have size 1
            refresh.head shouldContainAll keyValues
            println
        }
      }
    }

    "return all key-values when removeDeletes is false and when Segment was created in same Level" in {
      runThis(5.times, log = true) {
        TestCaseSweeper {
          implicit sweeper =>

            TestSweeper.createMemorySweeperMax().value.sweep()

            val keyValues: Slice[Memory.Put] = (1 to (randomIntMax(100000) max 2)).map(key => Memory.put(key, key, 1.second)).toSlice

            val enableCompression = randomBoolean()

            val shouldPrefixCompress = randomBoolean()

            val valuesConfig = ValuesBlock.Config.random(hasCompression = enableCompression)
            val sortedIndexConfig = SortedIndexBlock.Config.random(hasCompression = enableCompression, shouldPrefixCompress = shouldPrefixCompress)
            val binarySearchIndexConfig = BinarySearchIndexBlock.Config.random(hasCompression = enableCompression)
            val hashIndexConfig = HashIndexBlock.Config.random(hasCompression = enableCompression)
            val bloomFilterConfig = BloomFilterBlock.Config.random(hasCompression = enableCompression)

            val segmentConfig: SegmentBlock.Config =
              SegmentBlock.Config.random(
                hasCompression = enableCompression,
                minSegmentSize = Int.MaxValue,
                maxKeyValuesPerSegment = if (memory) keyValues.size else randomIntMax(keyValues.size)
              )

            val segment =
              Benchmark(s"Created Segment: ${keyValues.size} keyValues", inlinePrint = true) {
                TestSegment(
                  keyValues = keyValues,
                  createdInLevel = 5,
                  valuesConfig = valuesConfig,
                  sortedIndexConfig = sortedIndexConfig,
                  binarySearchIndexConfig = binarySearchIndexConfig,
                  hashIndexConfig = hashIndexConfig,
                  bloomFilterConfig = bloomFilterConfig,
                  segmentConfig = segmentConfig
                )
              }

            segment.getKeyValueCount() shouldBe keyValues.size

            sleep(2.seconds)

            val refresh =
              Benchmark(s"Refresh Segment: ${keyValues.size} keyValues", inlinePrint = true) {
                segment.refresh(
                  removeDeletes = false,
                  createdInLevel = 5,
                  valuesConfig = valuesConfig,
                  sortedIndexConfig = sortedIndexConfig,
                  binarySearchIndexConfig = binarySearchIndexConfig,
                  hashIndexConfig = hashIndexConfig,
                  bloomFilterConfig = bloomFilterConfig,
                  segmentConfig = segmentConfig.copy(minSize = Int.MaxValue)
                ).map(_.sweep())
              }

            refresh should have size 1
            refresh.head shouldContainAll keyValues
            println
        }
      }
    }
  }
}