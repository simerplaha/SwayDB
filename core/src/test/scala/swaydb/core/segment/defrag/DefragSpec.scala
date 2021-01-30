///*
// * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
// *
// * This file is a part of SwayDB.
// *
// * SwayDB is free software: you can redistribute it and/or modify
// * it under the terms of the GNU Affero General Public License as
// * published by the Free Software Foundation, either version 3 of the
// * License, or (at your option) any later version.
// *
// * SwayDB is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// * GNU Affero General Public License for more details.
// *
// * You should have received a copy of the GNU Affero General Public License
// * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
// *
// * Additional permission under the GNU Affero GPL version 3 section 7:
// * If you modify this Program or any covered work, only by linking or combining
// * it with separate works, the licensors of this Program grant you additional
// * permission to convey the resulting work.
// */
//
//package swaydb.core.segment.defrag
//
//import org.scalamock.scalatest.MockFactory
//import org.scalatest.EitherValues
//import swaydb.core.CommonAssertions._
//import swaydb.core.TestData._
//import swaydb.core.data.KeyValue
//import swaydb.core.segment.Segment
//import swaydb.core.segment.block.binarysearch.BinarySearchIndexBlock
//import swaydb.core.segment.block.bloomfilter.BloomFilterBlock
//import swaydb.core.segment.block.hashindex.HashIndexBlock
//import swaydb.core.segment.block.segment.SegmentBlock
//import swaydb.core.segment.block.segment.data.TransientSegment
//import swaydb.core.segment.block.sortedindex.SortedIndexBlock
//import swaydb.core.segment.block.values.ValuesBlock
//import swaydb.core.{TestBase, TestCaseSweeper, TestExecutionContext, TestTimer}
//import swaydb.testkit.RunThis._
//import swaydb.data.order.{KeyOrder, TimeOrder}
//import swaydb.data.slice.Slice
//import swaydb.serializers.Default._
//import swaydb.serializers._
//
//import scala.collection.mutable.ListBuffer
//
//class DefragSpec extends TestBase with MockFactory with EitherValues {
//
//  implicit val ec = TestExecutionContext.executionContext
//  implicit val timer = TestTimer.Empty
//
//  implicit val keyOrder = KeyOrder.default
//  implicit val timerOrder = TimeOrder.long
//
//  implicit def valuesConfig: ValuesBlock.Config = ValuesBlock.Config.random
//  implicit def sortedIndexConfig: SortedIndexBlock.Config = SortedIndexBlock.Config.random
//  implicit def binarySearchIndexConfig: BinarySearchIndexBlock.Config = BinarySearchIndexBlock.Config.random
//  implicit def hashIndexConfig: HashIndexBlock.Config = HashIndexBlock.Config.random
//  implicit def bloomFilterConfig: BloomFilterBlock.Config = BloomFilterBlock.Config.random
//  implicit def segmentConfig: SegmentBlock.Config = SegmentBlock.Config.random
//
//  "defrag" when {
//    "there are no gaps" when {
//      "removeDeletes = false" in {
//        runThis(10.times, log = true) {
//          TestCaseSweeper {
//            implicit sweeper =>
//
//              val segment = TestSegment()
//
//              val mergeResult =
//                Defrag.run(
//                  segment = Some(segment),
//                  nullSegment = Segment.Null,
//                  fragments = ListBuffer.empty,
//                  headGap = ListBuffer.empty,
//                  tailGap = ListBuffer.empty,
//                  mergeableCount = segment.keyValueCount,
//                  mergeable = segment.iterator(),
//                  removeDeletes = false,
//                  createdInLevel = 1
//                )
//
//              mergeResult.source shouldBe segment
//              mergeResult.result should have size 1
//              val stats = mergeResult.result.head.asInstanceOf[TransientSegment.Stats]
//              stats.stats.keyValues shouldBe segment.iterator().toList
//          }
//        }
//      }
//
//      "removeDeletes = true" in {
//        runThis(10.times, log = true) {
//          TestCaseSweeper {
//            implicit sweeper =>
//              val segment = TestSegment(keyValues = Slice(randomUpdateKeyValue(1), randomFunctionKeyValue(2), randomRemoveAny(3, 10)))
//
//              segment.hasUpdateOrRange shouldBe true
//
//              val mergeResult =
//                Defrag.run(
//                  segment = Some(segment),
//                  nullSegment = Segment.Null,
//                  fragments = ListBuffer.empty,
//                  headGap = ListBuffer.empty,
//                  tailGap = ListBuffer.empty,
//                  mergeableCount = segment.keyValueCount,
//                  mergeable = segment.iterator(),
//                  removeDeletes = true,
//                  createdInLevel = 1
//                )
//
//              mergeResult.source shouldBe segment
//              mergeResult.result shouldBe empty
//          }
//        }
//      }
//    }
//
//    "there are no key-values to merge" when {
//      "removeDeletes = false" in {
//        runThis(10.times, log = true) {
//          TestCaseSweeper {
//            implicit sweeper =>
//
//              val keyValues = randomKeyValues(30).groupedSlice(3)
//              keyValues should have size 3
//
//              val headGap = TestSegment(keyValues.head)
//              val segment = TestSegment(keyValues = keyValues.get(1))
//              val tailGap = TestSegment(keyValues.head)
//
//              implicit def segmentConfig: SegmentBlock.Config =
//                SegmentBlock.Config.random.copy(minSize = segment.segmentSize, maxCount = segment.keyValueCount)
//
//              val mergeResult =
//                Defrag.run(
//                  segment = Some(segment),
//                  nullSegment = Segment.Null,
//                  fragments = ListBuffer.empty,
//                  headGap = ListBuffer(headGap),
//                  tailGap = ListBuffer(tailGap),
//                  mergeableCount = 0,
//                  mergeable = Iterator.empty,
//                  removeDeletes = false,
//                  createdInLevel = 1
//                )
//
//              mergeResult.source shouldBe Segment.Null
//
//              mergeResult.result should have size 3
//
//              mergeResult.result.head.asInstanceOf[TransientSegment.RemoteSegment].segment shouldBe headGap
//              mergeResult.result.drop(1).head shouldBe TransientSegment.Fence
//              mergeResult.result.last.asInstanceOf[TransientSegment.RemoteSegment].segment shouldBe tailGap
//          }
//        }
//      }
//
//      "segmentSize is too small" in {
//        runThis(10.times, log = true) {
//          TestCaseSweeper {
//            implicit sweeper =>
//
//              val keyValues = randomPutKeyValues(count = 10000, startId = Some(0), valueSize = 0, addPutDeadlines = false).groupedSlice(1000)
//              keyValues should have size 1000
//
//              val headGap = TestSegment(keyValues.head)
//              val midSegment = TestSegment(keyValues = keyValues.dropHead().take(50).flatten) //
//              val tailGap = TestSegment(keyValues.drop(51).flatten) //make tail large so that it does not get expanded
//
//              implicit val valuesConfig: ValuesBlock.Config = ValuesBlock.Config.random
//              implicit val sortedIndexConfig: SortedIndexBlock.Config = SortedIndexBlock.Config.random
//              implicit val binarySearchIndexConfig: BinarySearchIndexBlock.Config = BinarySearchIndexBlock.Config.random
//              implicit val hashIndexConfig: HashIndexBlock.Config = HashIndexBlock.Config.random
//              implicit val bloomFilterConfig: BloomFilterBlock.Config = BloomFilterBlock.Config.random
//              implicit val segmentConfig: SegmentBlock.Config = SegmentBlock.Config.random.copy(minSize = midSegment.segmentSize + 1, maxCount = midSegment.keyValueCount + 1)
//
//              val allSegments = Seq(headGap, midSegment, tailGap)
//
//              val mergeResult =
//                Defrag.run(
//                  segment = Some(midSegment),
//                  nullSegment = Segment.Null,
//                  fragments = ListBuffer.empty,
//                  headGap = ListBuffer(headGap),
//                  tailGap = ListBuffer(tailGap),
//                  mergeableCount = 0,
//                  mergeable = Iterator.empty,
//                  removeDeletes = false,
//                  createdInLevel = 1
//                )
//
//              mergeResult.source shouldBe midSegment
//
//              mergeResult.result should have size 2
//
//              mergeResult.result.head.asInstanceOf[TransientSegment.Stats].stats.keyValues shouldBe keyValues.take(51).flatten
//
//              val defragKeyValues: ListBuffer[KeyValue] =
//                mergeResult.result flatMap {
//                  case remote: TransientSegment.Remote =>
//                    remote match {
//                      case ref: TransientSegment.RemoteRef =>
//                        ref.iterator().toList
//
//                      case segment: TransientSegment.RemoteSegment =>
//                        segment.iterator().toList
//                    }
//
//                  case TransientSegment.Fence =>
//                    fail(s"Did not expect a ${TransientSegment.Fence.productPrefix}")
//
//                  case TransientSegment.Stats(stats) =>
//                    stats.keyValues
//                }
//
//              defragKeyValues shouldBe allSegments.flatMap(_.iterator())
//          }
//        }
//      }
//    }
//  }
//}
