/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb.core.segment.defrag

import org.scalamock.scalatest.MockFactory
import org.scalatest.EitherValues
import swaydb.core.CommonAssertions._
import swaydb.core.TestData._
import swaydb.core.data.{KeyValue, Memory}
import swaydb.core.merge.stats.{MergeStats, MergeStatsCreator, MergeStatsSizeCalculator}
import swaydb.core.segment._
import swaydb.core.segment.block.binarysearch.{BinarySearchIndexBlock, BinarySearchIndexConfig}
import swaydb.core.segment.block.bloomfilter.{BloomFilterBlock, BloomFilterConfig}
import swaydb.core.segment.block.hashindex.HashIndexBlock
import swaydb.core.segment.block.segment.SegmentBlock
import swaydb.core.segment.block.segment.data.TransientSegment
import swaydb.core.segment.block.sortedindex.SortedIndexBlock
import swaydb.core.segment.block.values.ValuesBlock
import swaydb.core.{TestBase, TestCaseSweeper, TestExecutionContext, TestTimer}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.testkit.RunThis._

import scala.collection.mutable.ListBuffer

/**
 * Test setup for when input types are [[PersistentSegment]]
 */
class PersistentSegment_DefragSpec extends DefragSpec[PersistentSegment, PersistentSegmentOption, MergeStats.Persistent.Builder[Memory, ListBuffer]] {

  override def testSegment(keyValues: Slice[Memory])(implicit sweeper: TestCaseSweeper): PersistentSegment =
    TestSegment(keyValues).shouldBeInstanceOf[PersistentSegment]

  override def nullSegment: PersistentSegmentOption =
    PersistentSegment.Null

  override implicit def mergeStatsCreator: MergeStatsCreator[MergeStats.Persistent.Builder[Memory, ListBuffer]] =
    MergeStatsCreator.PersistentCreator

  override implicit def mergeStatsSizeCalculator(implicit sortedIndexConfig: SortedIndexBlock.Config): MergeStatsSizeCalculator[MergeStats.Persistent.Builder[Memory, ListBuffer]] =
    MergeStatsSizeCalculator.persistentSizeCalculator(sortedIndexConfig)
}

/**
 * Test setup for when input types are [[MemorySegment]]
 */
class MemorySegment_DefragSpec extends DefragSpec[MemorySegment, MemorySegmentOption, MergeStats.Memory.Builder[Memory, ListBuffer]] {

  override def inMemoryStorage = true

  override def testSegment(keyValues: Slice[Memory])(implicit sweeper: TestCaseSweeper): MemorySegment =
    TestSegment(keyValues).shouldBeInstanceOf[MemorySegment]

  override def nullSegment: MemorySegmentOption =
    MemorySegment.Null

  override implicit def mergeStatsCreator: MergeStatsCreator[MergeStats.Memory.Builder[Memory, ListBuffer]] =
    MergeStatsCreator.MemoryCreator

  override implicit def mergeStatsSizeCalculator(implicit sortedIndexConfig: SortedIndexBlock.Config): MergeStatsSizeCalculator[MergeStats.Memory.Builder[Memory, ListBuffer]] =
    MergeStatsSizeCalculator.MemoryCreator
}


sealed trait DefragSpec[SEG <: Segment, NULL_SEG >: SEG, S >: Null <: MergeStats.Segment[Memory, ListBuffer]] extends TestBase with MockFactory with EitherValues {

  implicit val ec = TestExecutionContext.executionContext
  implicit val timer = TestTimer.Empty

  implicit val keyOrder = KeyOrder.default
  implicit val timerOrder = TimeOrder.long

  implicit def valuesConfig: ValuesBlock.Config = ValuesBlock.Config.random
  implicit def sortedIndexConfig: SortedIndexBlock.Config = SortedIndexBlock.Config.random
  implicit def binarySearchIndexConfig: BinarySearchIndexConfig = BinarySearchIndexConfig.random
  implicit def hashIndexConfig: HashIndexBlock.Config = HashIndexBlock.Config.random
  implicit def bloomFilterConfig: BloomFilterConfig = BloomFilterConfig.random
  implicit def segmentConfig: SegmentBlock.Config = SegmentBlock.Config.random

  def testSegment(keyValues: Slice[Memory] = randomizedKeyValues())(implicit sweeper: TestCaseSweeper): SEG
  def nullSegment: NULL_SEG
  implicit def mergeStatsCreator: MergeStatsCreator[S]
  implicit def mergeStatsSizeCalculator(implicit sortedIndexConfig: SortedIndexBlock.Config): MergeStatsSizeCalculator[S]

  "defrag" when {
    "there are no gaps" when {
      "removeDeletes = false" in {
        runThis(10.times, log = true) {
          TestCaseSweeper {
            implicit sweeper =>

              //ignore pending apply, functions and ranges since they merge update key-values to a compressed format
              val segment = testSegment(randomizedKeyValues(addPendingApply = false, addFunctions = false, addRanges = false))

              val mergeResult =
                Defrag.runOnSegment[SEG, NULL_SEG, S](
                  segment = segment,
                  nullSegment = nullSegment,
                  fragments = ListBuffer.empty,
                  headGap = ListBuffer.empty,
                  tailGap = ListBuffer.empty,
                  newKeyValues = segment.iterator(randomBoolean()),
                  removeDeletes = false,
                  createdInLevel = 1,
                  createFence = _ => TransientSegment.Fence
                )

              //expect no key-values to change
              mergeResult.input shouldBe segment
              mergeResult.output should have size 1
              val stats = mergeResult.output.head.shouldBeInstanceOf[TransientSegment.Stats[S]]
              stats.stats.keyValues shouldBe segment.iterator(randomBoolean()).toList
          }
        }
      }

      "removeDeletes = true" in {
        runThis(10.times, log = true) {
          TestCaseSweeper {
            implicit sweeper =>
              //add only updated key-values
              val segment = testSegment(keyValues = Slice(randomUpdateKeyValue(1), randomFunctionKeyValue(2), randomRemoveAny(3, 10)))

              segment.hasUpdateOrRange shouldBe true

              val mergeResult =
                Defrag.runOnSegment[SEG, NULL_SEG, S](
                  segment = segment,
                  nullSegment = nullSegment,
                  fragments = ListBuffer.empty,
                  headGap = ListBuffer.empty,
                  tailGap = ListBuffer.empty,
                  newKeyValues = segment.iterator(randomBoolean()),
                  removeDeletes = true,
                  createdInLevel = 1,
                  createFence = _ => TransientSegment.Fence
                )

              mergeResult.input shouldBe segment
              mergeResult.output shouldBe empty
          }
        }
      }
    }

    "there are no key-values to merge" when {
      "removeDeletes = false" in {
        runThis(10.times, log = true) {
          TestCaseSweeper {
            implicit sweeper =>

              val keyValues = randomKeyValues(30).groupedSlice(3)
              keyValues should have size 3

              val headGap = testSegment(keyValues.head)
              val segment = testSegment(keyValues = keyValues.get(1))
              val tailGap = testSegment(keyValues.head)

              implicit def segmentConfig: SegmentBlock.Config =
                SegmentBlock.Config.random.copy(minSize = segment.segmentSize, maxCount = segment.keyValueCount)

              val mergeResult =
                Defrag.runOnSegment[SEG, NULL_SEG, S](
                  segment = segment,
                  nullSegment = nullSegment,
                  fragments = ListBuffer.empty,
                  headGap = ListBuffer(headGap),
                  tailGap = ListBuffer(tailGap),
                  newKeyValues = Iterator.empty,
                  removeDeletes = false,
                  createdInLevel = 1,
                  createFence = _ => TransientSegment.Fence
                )

              mergeResult.input shouldBe nullSegment

              mergeResult.output should have size 3

              if (persistent) //if it's persistent Remote Segments instances
                mergeResult.output.head.shouldBeInstanceOf[TransientSegment.RemotePersistentSegment].segment shouldBe headGap
              else //if it's memory stats are created
                mergeResult.output.head.shouldBeInstanceOf[TransientSegment.Stats[S]].stats.keyValues shouldBe headGap.iterator(randomBoolean()).toList

              //fence always remains the same
              mergeResult.output.drop(1).head shouldBe TransientSegment.Fence

              if (persistent) //if it's persistent Remote Segments instances
                mergeResult.output.last.shouldBeInstanceOf[TransientSegment.RemotePersistentSegment].segment shouldBe tailGap
              else //if it's memory stats are created
                mergeResult.output.last.shouldBeInstanceOf[TransientSegment.Stats[S]].stats.keyValues shouldBe tailGap.iterator(randomBoolean()).toList
          }
        }
      }

      "segmentSize is too small" in {
        runThis(10.times, log = true) {
          TestCaseSweeper {
            implicit sweeper =>

              val keyValues = randomPutKeyValues(count = 10000, startId = Some(0), valueSize = 0, addPutDeadlines = false).groupedSlice(1000)
              keyValues should have size 1000

              val headGap = testSegment(keyValues.head)
              val midSegment = testSegment(keyValues = keyValues.dropHead().take(50).flatten)
              val tailGap = testSegment(keyValues.drop(51).flatten) //make tail large so that it does not get expanded

              implicit val valuesConfig: ValuesBlock.Config = ValuesBlock.Config.random
              implicit val sortedIndexConfig: SortedIndexBlock.Config = SortedIndexBlock.Config.random
              implicit val binarySearchIndexConfig: BinarySearchIndexConfig = BinarySearchIndexConfig.random
              implicit val hashIndexConfig: HashIndexBlock.Config = HashIndexBlock.Config.random
              implicit val bloomFilterConfig: BloomFilterConfig = BloomFilterConfig.random
              implicit val segmentConfig: SegmentBlock.Config = SegmentBlock.Config.random.copy(minSize = midSegment.segmentSize + 1, maxCount = midSegment.keyValueCount + 1)

              val allSegments = Seq(headGap, midSegment, tailGap)

              val mergeResult =
                Defrag.runOnSegment[SEG, NULL_SEG, S](
                  segment = midSegment,
                  nullSegment = nullSegment,
                  fragments = ListBuffer.empty,
                  headGap = ListBuffer(headGap),
                  tailGap = ListBuffer(tailGap),
                  newKeyValues = Iterator.empty,
                  removeDeletes = false,
                  createdInLevel = 1,
                  createFence = _ => TransientSegment.Fence
                )

              mergeResult.input shouldBe midSegment

              if (persistent) {
                mergeResult.output should have size 2

                mergeResult.output.head.shouldBeInstanceOf[TransientSegment.Stats[S]].stats.keyValues shouldBe keyValues.take(51).flatten

                mergeResult.output.last.shouldBeInstanceOf[TransientSegment.RemotePersistentSegment].segment shouldBe tailGap

                //collect all key-values and they should be the same as input Segments
                val defragKeyValues: ListBuffer[KeyValue] =
                  mergeResult.output flatMap {
                    case remote: TransientSegment.Remote =>
                      remote match {
                        case ref: TransientSegment.RemoteRef =>
                          ref.iterator(randomBoolean()).toList

                        case segment: TransientSegment.RemotePersistentSegment =>
                          segment.iterator(randomBoolean()).toList
                      }

                    case TransientSegment.Fence =>
                      fail(s"Did not expect a ${TransientSegment.Fence.productPrefix}")

                    case TransientSegment.Stats(stats) =>
                      stats.keyValues
                  }

                defragKeyValues shouldBe allSegments.flatMap(_.iterator(randomBoolean()))
              } else {
                //Memory Segments aren't that smart right now. They just always expand.
                mergeResult.output should have size 1
                mergeResult.output.head.shouldBeInstanceOf[TransientSegment.Stats[S]].stats.keyValues shouldBe keyValues.flatten
              }
          }
        }
      }
    }
  }
}
