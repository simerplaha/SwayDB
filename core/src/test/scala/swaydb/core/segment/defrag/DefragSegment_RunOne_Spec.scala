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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.core.segment.defrag

import org.scalamock.scalatest.MockFactory
import org.scalatest.EitherValues
import swaydb.core.CommonAssertions._
import swaydb.core.TestData._
import swaydb.core.data.Memory
import swaydb.core.merge.MergeStats
import swaydb.core.segment.Segment
import swaydb.core.segment.assigner.Assignable
import swaydb.core.segment.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.block.bloomfilter.BloomFilterBlock
import swaydb.core.segment.block.hashindex.HashIndexBlock
import swaydb.core.segment.block.segment.SegmentBlock
import swaydb.core.segment.block.segment.data.TransientSegment
import swaydb.core.segment.block.sortedindex.SortedIndexBlock
import swaydb.core.segment.block.values.ValuesBlock
import swaydb.core.segment.io.SegmentReadIO
import swaydb.core.{TestBase, TestCaseSweeper, TestExecutionContext, TestTimer}
import swaydb.data.RunThis._
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.collection.mutable.ListBuffer

class DefragSegment_RunOne_Spec extends TestBase with MockFactory with EitherValues {

  implicit val ec = TestExecutionContext.executionContext
  implicit val timer = TestTimer.Empty

  implicit val keyOrder = KeyOrder.default
  implicit val timerOrder = TimeOrder.long
  implicit def segmentReadIO = SegmentReadIO.random

  "NO GAPS - no key-values to merge" should {
    "result in empty" in {
      runThis(20.times, log = true) {
        TestCaseSweeper {
          implicit sweeper =>

            //HEAD - EMPTY
            //MID  - EMPTY
            //GAP  - EMPTY

            //SEG  - [1 - 10]

            implicit val valuesConfig: ValuesBlock.Config = ValuesBlock.Config.random
            implicit val sortedIndexConfig: SortedIndexBlock.Config = SortedIndexBlock.Config.random
            implicit val binarySearchIndexConfig: BinarySearchIndexBlock.Config = BinarySearchIndexBlock.Config.random
            implicit val hashIndexConfig: HashIndexBlock.Config = HashIndexBlock.Config.random
            implicit val bloomFilterConfig: BloomFilterBlock.Config = BloomFilterBlock.Config.random
            implicit val segmentConfig: SegmentBlock.Config = SegmentBlock.Config.random

            val segment = TestSegment()

            val mergeResult =
              DefragSegment.runOne(
                segment = Some(segment),
                nullSegment = Segment.Null,
                headGap = ListBuffer.empty,
                tailGap = ListBuffer.empty,
                mergeableCount = 0,
                mergeable = Iterator.empty,
                removeDeletes = false,
                createdInLevel = 1
              ).await

            mergeResult.source shouldBe Segment.Null
            mergeResult.result shouldBe empty
        }
      }
    }
  }

  "NO GAPS - Segment gets merged into itself own key-values" in {
    runThis(20.times, log = true) {
      TestCaseSweeper {
        implicit sweeper =>

          //HEAD - EMPTY
          //MID  - [1 - 10]
          //GAP  - EMPTY

          //SEG  - [1 - 10]

          implicit val valuesConfig: ValuesBlock.Config = ValuesBlock.Config.random
          implicit val sortedIndexConfig: SortedIndexBlock.Config = SortedIndexBlock.Config.random
          implicit val binarySearchIndexConfig: BinarySearchIndexBlock.Config = BinarySearchIndexBlock.Config.random
          implicit val hashIndexConfig: HashIndexBlock.Config = HashIndexBlock.Config.random
          implicit val bloomFilterConfig: BloomFilterBlock.Config = BloomFilterBlock.Config.random
          implicit val segmentConfig: SegmentBlock.Config = SegmentBlock.Config.random

          val segment = TestSegment()

          val mergeResult =
            DefragSegment.runOne(
              segment = Some(segment),
              nullSegment = Segment.Null,
              headGap = ListBuffer.empty,
              tailGap = ListBuffer.empty,
              mergeableCount = segment.keyValueCount,
              mergeable = segment.iterator(),
              removeDeletes = false,
              createdInLevel = 1
            ).await

          mergeResult.source shouldBe segment
          mergeResult.result should have size 1

          val newSegments = mergeResult.result.persist(createPathDistributor).get
          newSegments.iterator.flatMap(_.iterator()).toList shouldBe segment.iterator().toList
      }
    }
  }

  "Segment gets merged into itself and removeDeletes = true" in {
    runThis(20.times, log = true) {
      TestCaseSweeper {
        implicit sweeper =>
          //all key-values are removable so it doesn't matter if it contains gaps or not all key-values should get cleared.

          //HEAD - EMPTY | [1,2,3]
          //MID  - [4,5,6]
          //GAP  - EMPTY | [7,8,9]

          //SEG  - [4,5,6]

          implicit val valuesConfig: ValuesBlock.Config = ValuesBlock.Config.random
          implicit val sortedIndexConfig: SortedIndexBlock.Config = SortedIndexBlock.Config.random
          implicit val binarySearchIndexConfig: BinarySearchIndexBlock.Config = BinarySearchIndexBlock.Config.random
          implicit val hashIndexConfig: HashIndexBlock.Config = HashIndexBlock.Config.random
          implicit val bloomFilterConfig: BloomFilterBlock.Config = BloomFilterBlock.Config.random
          implicit val segmentConfig: SegmentBlock.Config = SegmentBlock.Config.random

          lazy val headSegment = TestSegment(keyValues = Slice(randomUpdateKeyValue(1), randomRemoveAny(2, 3)))
          val midSegment = TestSegment(keyValues = Slice(randomUpdateKeyValue(4), randomRemoveAny(5, 6)))
          lazy val tailSegment = TestSegment(keyValues = Slice(randomUpdateKeyValue(7), randomRemoveAny(8, 9)))

          val headGap: ListBuffer[Assignable.Gap[MergeStats.Persistent.Builder[Memory, ListBuffer]]] =
            eitherOne(ListBuffer.empty, ListBuffer(headSegment))

          val tailGap: ListBuffer[Assignable.Gap[MergeStats.Persistent.Builder[Memory, ListBuffer]]] =
            eitherOne(ListBuffer.empty, ListBuffer(tailSegment))

          val (mergeableCount, mergeable) =
            eitherOne(
              (0, Iterator.empty),
              (midSegment.keyValueCount, midSegment.iterator())
            )

          val mergeResult =
            DefragSegment.runOne(
              segment = Some(midSegment),
              nullSegment = Segment.Null,
              headGap = headGap,
              tailGap = tailGap,
              mergeableCount = mergeableCount,
              mergeable = mergeable,
              removeDeletes = true,
              createdInLevel = 1
            ).await

          mergeResult.source shouldBe midSegment
          mergeResult.result shouldBe empty
      }
    }
  }

  "HEAD GAP only" in {
    runThis(20.times, log = true) {
      TestCaseSweeper {
        implicit sweeper =>
          //all key-values are removable so it doesn't matter if it contains gaps or not all key-values should get cleared.

          //HEAD - [0 - 49]
          //MID  - EMPTY
          //GAP  - EMPTY

          //SEG  - [50 - 99]

          implicit val valuesConfig: ValuesBlock.Config = ValuesBlock.Config.random
          implicit val sortedIndexConfig: SortedIndexBlock.Config = SortedIndexBlock.Config.random
          implicit val binarySearchIndexConfig: BinarySearchIndexBlock.Config = BinarySearchIndexBlock.Config.random
          implicit val hashIndexConfig: HashIndexBlock.Config = HashIndexBlock.Config.random
          implicit val bloomFilterConfig: BloomFilterBlock.Config = BloomFilterBlock.Config.random

          val keyValues = randomPutKeyValues(100, startId = Some(0)).groupedSlice(2)
          keyValues should have size 2

          val headSegment = TestSegment.one(keyValues = keyValues.head)
          val midSegment = TestSegment.one(keyValues = keyValues.last)

          implicit val segmentConfig: SegmentBlock.Config = SegmentBlock.Config.random.copy(minSize = headSegment.segmentSize min midSegment.segmentSize)

          val removeDeletes = randomBoolean()
          val createdInLevel = randomIntMax(100)

          val mergeResult =
            DefragSegment.runOne(
              segment = Some(midSegment),
              nullSegment = Segment.Null,
              headGap = ListBuffer(headSegment),
              tailGap = ListBuffer.empty,
              mergeableCount = 0,
              mergeable = Iterator.empty,
              removeDeletes = removeDeletes,
              createdInLevel = createdInLevel
            ).await

          mergeResult.source shouldBe Segment.Null
          mergeResult.result should have size 1
          mergeResult.result should contain only
            TransientSegment.RemoteSegment(
              segment = headSegment,
              removeDeletes = removeDeletes,
              createdInLevel = createdInLevel,
              valuesConfig = valuesConfig,
              sortedIndexConfig = sortedIndexConfig,
              binarySearchIndexConfig = binarySearchIndexConfig,
              hashIndexConfig = hashIndexConfig,
              bloomFilterConfig = bloomFilterConfig,
              segmentConfig = segmentConfig
            )

          mergeResult.result.persist(createPathDistributor).get.flatMap(_.iterator()).toList shouldBe headSegment.iterator().toList
      }
    }
  }

  "MULTIPLE HEAD GAPs" in {
    runThis(20.times, log = true) {
      TestCaseSweeper {
        implicit sweeper =>
          //all key-values are removable so it doesn't matter if it contains gaps or not all key-values should get cleared.

          //HEAD - [0 - 10, 11 - 20, 21 - 30, 31 - 40, 41 - 50]
          //MID  - EMPTY
          //GAP  - EMPTY

          //SEG  - [51 - 99]

          implicit val valuesConfig: ValuesBlock.Config = ValuesBlock.Config.random
          implicit val sortedIndexConfig: SortedIndexBlock.Config = SortedIndexBlock.Config.random
          implicit val binarySearchIndexConfig: BinarySearchIndexBlock.Config = BinarySearchIndexBlock.Config.random
          implicit val hashIndexConfig: HashIndexBlock.Config = HashIndexBlock.Config.random
          implicit val bloomFilterConfig: BloomFilterBlock.Config = BloomFilterBlock.Config.random

          val keyValues = randomPutKeyValues(100, startId = Some(0)).groupedSlice(10)
          keyValues should have size 10

          val headSegments: Slice[Segment] =
            keyValues.take(5) map {
              keyValues =>
                TestSegment.one(keyValues = keyValues)
            }

          headSegments should have size 5

          val midSegment = TestSegment.one(keyValues = keyValues.drop(headSegments.size).flatten)

          val minSize = headSegments.map(_.segmentSize).min min midSegment.segmentSize
          implicit val segmentConfig: SegmentBlock.Config = SegmentBlock.Config.random.copy(minSize = minSize)

          val removeDeletes = randomBoolean()
          val createdInLevel = randomIntMax(100)

          val mergeResult =
            DefragSegment.runOne(
              segment = Some(midSegment),
              nullSegment = Segment.Null,
              headGap = ListBuffer.from(headSegments),
              tailGap = ListBuffer.empty,
              mergeableCount = 0,
              mergeable = Iterator.empty,
              removeDeletes = removeDeletes,
              createdInLevel = createdInLevel
            ).await

          val expectedTransientSegments =
            headSegments map {
              segment =>
                TransientSegment.RemoteSegment(
                  segment = segment,
                  removeDeletes = removeDeletes,
                  createdInLevel = createdInLevel,
                  valuesConfig = valuesConfig,
                  sortedIndexConfig = sortedIndexConfig,
                  binarySearchIndexConfig = binarySearchIndexConfig,
                  hashIndexConfig = hashIndexConfig,
                  bloomFilterConfig = bloomFilterConfig,
                  segmentConfig = segmentConfig
                )
            }

          mergeResult.source shouldBe Segment.Null
          mergeResult.result shouldBe expectedTransientSegments

          val persistedSegments = mergeResult.result.persist(createPathDistributor).get

          persistedSegments.flatMap(_.iterator()).toList shouldBe headSegments.flatMap(_.iterator()).toList
          persistedSegments.foreach(_.clearAllCaches())
          persistedSegments.flatMap(_.iterator()).toList shouldBe headSegments.flatMap(_.iterator()).toList
      }
    }
  }

  "MULTIPLE HEAD and TAIL GAPs" in {
    runThis(20.times, log = true) {
      TestCaseSweeper {
        implicit sweeper =>
          //all key-values are removable so it doesn't matter if it contains gaps or not all key-values should get cleared.

          //HEAD - [0 - 10, 11 - 20, 21 - 30, 31 - 40, 41 - 50]
          //MID  - EMPTY
          //GAP  - [71 - 80, 81 - 90]

          //SEG  - [51 - 60, 61 - 70]

          implicit val valuesConfig: ValuesBlock.Config = ValuesBlock.Config.random
          implicit val sortedIndexConfig: SortedIndexBlock.Config = SortedIndexBlock.Config.random
          implicit val binarySearchIndexConfig: BinarySearchIndexBlock.Config = BinarySearchIndexBlock.Config.random
          implicit val hashIndexConfig: HashIndexBlock.Config = HashIndexBlock.Config.random
          implicit val bloomFilterConfig: BloomFilterBlock.Config = BloomFilterBlock.Config.random

          val keyValues = randomPutKeyValues(100, startId = Some(0)).groupedSlice(10)
          keyValues should have size 10

          val headSegments: Slice[Segment] =
            keyValues.take(5) map {
              keyValues =>
                TestSegment.one(keyValues = keyValues)
            }

          headSegments should have size 5

          val midSegment = TestSegment.one(keyValues = keyValues.drop(headSegments.size).take(2).flatten)

          val tailSegments: Slice[Segment] =
            keyValues.drop(7) map {
              keyValues =>
                TestSegment.one(keyValues = keyValues)
            }

          val minSize = (headSegments ++ tailSegments).map(_.segmentSize).min min midSegment.segmentSize
          implicit val segmentConfig: SegmentBlock.Config = SegmentBlock.Config.random.copy(minSize = minSize)

          val removeDeletes = randomBoolean()
          val createdInLevel = randomIntMax(100)

          val mergeResult =
            DefragSegment.runOne(
              segment = Some(midSegment),
              nullSegment = Segment.Null,
              headGap = ListBuffer.from(headSegments),
              tailGap = ListBuffer.from(tailSegments),
              mergeableCount = 0,
              mergeable = Iterator.empty,
              removeDeletes = removeDeletes,
              createdInLevel = createdInLevel
            ).await

          def createExpectedRemoteSegment(segment: Segment) =
            TransientSegment.RemoteSegment(
              segment = segment,
              removeDeletes = removeDeletes,
              createdInLevel = createdInLevel,
              valuesConfig = valuesConfig,
              sortedIndexConfig = sortedIndexConfig,
              binarySearchIndexConfig = binarySearchIndexConfig,
              hashIndexConfig = hashIndexConfig,
              bloomFilterConfig = bloomFilterConfig,
              segmentConfig = segmentConfig
            )

          val expectedTransientSegments =
            headSegments.map(createExpectedRemoteSegment) ++ tailSegments.map(createExpectedRemoteSegment)

          mergeResult.source shouldBe Segment.Null
          mergeResult.result shouldBe expectedTransientSegments

          val persistedSegments = mergeResult.result.persist(createPathDistributor).get

          persistedSegments.flatMap(_.iterator()).toList shouldBe (keyValues.take(5) ++ keyValues.drop(7)).flatten
          persistedSegments.foreach(_.clearAllCaches())
          persistedSegments.flatMap(_.iterator()).toList shouldBe (keyValues.take(5) ++ keyValues.drop(7)).flatten
      }
    }
  }

  "MULTIPLE HEAD and TAIL GAPs - that does not open mid segment" in {
    runThis(20.times, log = true) {
      TestCaseSweeper {
        implicit sweeper =>

          //HEAD - [0 - 10, 11 - 20, 21 - 30, 31 - 40, 41 - 50]
          //MID  - EMPTY
          //GAP  - [71 - 80, 81 - 90]

          //SEG  - [51 - 60, 61 - 70]

          implicit val valuesConfig: ValuesBlock.Config = ValuesBlock.Config.random
          implicit val sortedIndexConfig: SortedIndexBlock.Config = SortedIndexBlock.Config.random
          implicit val binarySearchIndexConfig: BinarySearchIndexBlock.Config = BinarySearchIndexBlock.Config.random
          implicit val hashIndexConfig: HashIndexBlock.Config = HashIndexBlock.Config.random
          implicit val bloomFilterConfig: BloomFilterBlock.Config = BloomFilterBlock.Config.random

          val keyValues = randomPutKeyValues(100, startId = Some(0)).groupedSlice(10)
          keyValues should have size 10

          val headSegments: Slice[Segment] =
            keyValues.take(5) map {
              keyValues =>
                TestSegment.one(keyValues = keyValues)
            }

          headSegments should have size 5

          val midSegment = TestSegment.one(keyValues = keyValues.drop(headSegments.size).take(2).flatten)

          val tailSegments: Slice[Segment] =
            keyValues.drop(7) map {
              keyValues =>
                TestSegment.one(keyValues = keyValues)
            }

          //segmentSize is always <= midSegment.segmentSize so that it does not get expanded. This test is to ensure
          //that gap Segments do not join when midSegments are not expanded.
          implicit val segmentConfig: SegmentBlock.Config = SegmentBlock.Config.random.copy(minSize = randomIntMax(midSegment.segmentSize))

          val removeDeletes = randomBoolean()
          val createdInLevel = randomIntMax(100)

          val mergeResult =
            DefragSegment.runOne(
              segment = Some(midSegment),
              nullSegment = Segment.Null,
              headGap = ListBuffer.from(headSegments),
              tailGap = ListBuffer.from(tailSegments),
              mergeableCount = 0,
              mergeable = Iterator.empty,
              removeDeletes = removeDeletes,
              createdInLevel = createdInLevel
            ).awaitInf

          mergeResult.source shouldBe Segment.Null

          mergeResult.result.exists(_.minKey == tailSegments.head.minKey) shouldBe true

          val persistedSegments = mergeResult.result.persist(createPathDistributor).get

          //there exists a Segment without tailSegment's minKey.
          persistedSegments.exists(_.minKey == tailSegments.head.minKey) shouldBe true

          persistedSegments.flatMap(_.iterator()).toList shouldBe (keyValues.take(5) ++ keyValues.drop(7)).flatten
          persistedSegments.foreach(_.clearAllCaches())
          persistedSegments.flatMap(_.iterator()).toList shouldBe (keyValues.take(5) ++ keyValues.drop(7)).flatten
      }
    }
  }

  "MULTIPLE HEAD and TAIL GAPs - which might open mid segment" in {
    runThis(20.times, log = true) {
      TestCaseSweeper {
        implicit sweeper =>

          //HEAD - [0 - 10, 11 - 20, 21 - 30, 31 - 40, 41 - 50]
          //MID  - EMPTY
          //GAP  - [71 - 80, 81 - 90]

          //SEG  - [51 - 60, 61 - 70]

          implicit val valuesConfig: ValuesBlock.Config = ValuesBlock.Config.random
          implicit val sortedIndexConfig: SortedIndexBlock.Config = SortedIndexBlock.Config.random
          implicit val binarySearchIndexConfig: BinarySearchIndexBlock.Config = BinarySearchIndexBlock.Config.random
          implicit val hashIndexConfig: HashIndexBlock.Config = HashIndexBlock.Config.random
          implicit val bloomFilterConfig: BloomFilterBlock.Config = BloomFilterBlock.Config.random

          val keyValues = randomPutKeyValues(100, startId = Some(0)).groupedSlice(10)
          keyValues should have size 10

          val headSegments: Slice[Segment] =
            keyValues.take(5) map {
              keyValues =>
                TestSegment.one(keyValues = keyValues)
            }

          headSegments should have size 5

          val midSegment = TestSegment.one(keyValues = keyValues.drop(headSegments.size).take(2).flatten)

          val tailSegments: Slice[Segment] =
            keyValues.drop(7) map {
              keyValues =>
                TestSegment.one(keyValues = keyValues)
            }

          val allSegments = headSegments ++ Seq(midSegment) ++ tailSegments

          //segmentSize to be something random which gives it a chance
          implicit val segmentConfig: SegmentBlock.Config = SegmentBlock.Config.random.copy(minSize = randomIntMax(allSegments.map(_.segmentSize).max * 2))

          val removeDeletes = randomBoolean()
          val createdInLevel = randomIntMax(100)

          val mergeResult =
            DefragSegment.runOne(
              segment = Some(midSegment),
              nullSegment = Segment.Null,
              headGap = ListBuffer.from(headSegments),
              tailGap = ListBuffer.from(tailSegments),
              mergeableCount = 0,
              mergeable = Iterator.empty,
              removeDeletes = removeDeletes,
              createdInLevel = createdInLevel
            ).awaitInf

          val persistedSegments = mergeResult.result.persist(createPathDistributor).get

          val expectedKeyValues =
            mergeResult.source match {
              case Segment.Null =>
                //segment was not replaced so only gap key-values exists.
                println(s"Segment: Segment.Null")
                (keyValues.take(5) ++ keyValues.drop(7)).flatten

              case segment: Segment =>
                //if segment was replaced and all key-values are expected in final segments.
                println(s"Segment: Segment.${segment.getClass.getSimpleName}")
                keyValues.flatten
            }

          persistedSegments.flatMap(_.iterator()).toList shouldBe expectedKeyValues
          persistedSegments.foreach(_.clearAllCaches())
          persistedSegments.flatMap(_.iterator()).toList shouldBe expectedKeyValues
      }
    }
  }
}
