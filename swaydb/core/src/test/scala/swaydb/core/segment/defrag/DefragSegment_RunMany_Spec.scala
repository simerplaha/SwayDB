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

import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec
import swaydb.config.MMAP
import swaydb.core.{CoreSpecType, CoreTestSweeper}
import swaydb.core.segment.{PersistentSegment, PersistentSegmentMany}
import swaydb.core.segment.assigner.Assignable
import swaydb.core.segment.block.binarysearch.BinarySearchIndexBlockConfig
import swaydb.core.segment.block.bloomfilter.BloomFilterBlockConfig
import swaydb.core.segment.block.hashindex.HashIndexBlockConfig
import swaydb.core.segment.block.segment.SegmentBlockConfig
import swaydb.core.segment.block.sortedindex.SortedIndexBlockConfig
import swaydb.core.segment.block.values.ValuesBlockConfig
import swaydb.core.segment.data.KeyValueTestKit._
import swaydb.core.segment.io.SegmentReadIO
import swaydb.core.segment.SegmentTestKit._
import swaydb.core.segment.block.SegmentBlockTestKit._
import swaydb.slice.Slice
import swaydb.slice.order.{KeyOrder, TimeOrder}
import swaydb.slice.SliceTestKit._
import swaydb.testkit.RunThis._
import swaydb.testkit.TestKit._
import swaydb.TestExecutionContext
import swaydb.config.CoreConfigTestKit._
import swaydb.core.log.timer.TestTimer
import swaydb.core.segment.data.SegmentKeyOrders
import swaydb.core.segment.distributor.PathDistributor

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext
import scala.util.Random

class DefragSegment_RunMany_Spec extends AnyWordSpec {

  implicit val ec: ExecutionContext = TestExecutionContext.executionContext
  implicit val timer: TestTimer = TestTimer.Empty

  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default
  implicit val segmentKeyOrder: SegmentKeyOrders = SegmentKeyOrders(keyOrder)
  implicit val timerOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  implicit def segmentReadIO: SegmentReadIO = SegmentReadIO.random

  "NO GAP - empty should result in empty" in {
    runThis(20.times, log = true) {
      CoreTestSweeper {
        implicit sweeper =>
          import sweeper._

          //HEAD - EMPTY
          //MID  - EMPTY
          //GAP  - EMPTY

          //SEG  - [0 - 99]

          implicit val coreSpecType: CoreSpecType = CoreSpecType.random() //either memory or persistent
          implicit val valuesConfig: ValuesBlockConfig = ValuesBlockConfig.random
          implicit val sortedIndexConfig: SortedIndexBlockConfig = SortedIndexBlockConfig.random
          implicit val binarySearchIndexConfig: BinarySearchIndexBlockConfig = BinarySearchIndexBlockConfig.random
          implicit val hashIndexConfig: HashIndexBlockConfig = HashIndexBlockConfig.random
          implicit val bloomFilterConfig: BloomFilterBlockConfig = BloomFilterBlockConfig.random
          implicit val segmentConfig: SegmentBlockConfig = SegmentBlockConfig.random.copy(maxCount = 10, minSize = Int.MaxValue)

          val keyValues = randomPutKeyValues(100, startId = Some(0))

          val segments =
            TestSegment.many(
              keyValues = keyValues,
              segmentConfig = segmentConfig
            )

          segments should have size 1
          val many = segments.head.asInstanceOf[PersistentSegmentMany]

          val mergeResult =
            DefragPersistentSegment.runMany(
              headGap = ListBuffer.empty,
              tailGap = ListBuffer.empty,
              segment = many,
              newKeyValues = Iterator.empty,
              removeDeletes = false,
              createdInLevel = 1,
              pathDistributor = sweeper.pathDistributor,
              segmentRefCacheLife = randomSegmentRefCacheLife(),
              mmap = MMAP.randomForSegment()
            ).await

          mergeResult.input shouldBe PersistentSegment.Null
          mergeResult.output.isEmpty shouldBe true
      }
    }
  }

  "NO GAP - randomly select key-values to merge" in {
    runThis(100.times, log = true) {
      CoreTestSweeper {
        implicit sweeper =>
          import sweeper._

          //HEAD - EMPTY
          //MID  - [0 - 99]
          //GAP  - EMPTY

          //SEG  - [0 - 99]

          implicit val coreSpecType: CoreSpecType = CoreSpecType.random() //either memory or persistent
          implicit val valuesConfig: ValuesBlockConfig = ValuesBlockConfig.random
          implicit val sortedIndexConfig: SortedIndexBlockConfig = SortedIndexBlockConfig.random
          implicit val binarySearchIndexConfig: BinarySearchIndexBlockConfig = BinarySearchIndexBlockConfig.random
          implicit val hashIndexConfig: HashIndexBlockConfig = HashIndexBlockConfig.random
          implicit val bloomFilterConfig: BloomFilterBlockConfig = BloomFilterBlockConfig.random
          implicit val segmentConfig: SegmentBlockConfig = SegmentBlockConfig.random.copy(maxCount = 10, minSize = Int.MaxValue)
          implicit val pathDistributor: PathDistributor = sweeper.pathDistributor

          val keyValues = randomPutKeyValues(100, startId = Some(0))

          val segments =
            TestSegment.many(
              keyValues = keyValues,
              segmentConfig = segmentConfig
            )

          segments should have size 1
          val many = segments.head.asInstanceOf[PersistentSegmentMany]

          val chance = eitherOne(0.50, 0.60, 0.70, 0.80, 0.90, 0.100)

          //randomly select key-values to merge.
          val keyValuesToMerge = {
            val keyValuesToMerge =
              keyValues collect {
                case keyValue if Random.nextDouble() >= chance => keyValue
              }

            //if keyValuesToMerge results in empty Slice then create a slice with at least one Segment.
            if (keyValuesToMerge.isEmpty)
              Slice(keyValues(randomIntMax(keyValues.size)))
            else
              keyValuesToMerge
          }

          val mergeResult =
            DefragPersistentSegment.runMany(
              headGap = ListBuffer.empty,
              tailGap = ListBuffer.empty,
              segment = many,
              newKeyValues = keyValuesToMerge.iterator,
              removeDeletes = false,
              createdInLevel = 1,
              pathDistributor = sweeper.pathDistributor,
              segmentRefCacheLife = randomSegmentRefCacheLife(),
              mmap = MMAP.randomForSegment()
            ).awaitInf

          mergeResult.input shouldBe many
          mergeResult.output.toSlice.flatMapToSliceSlow(_.iterator(randomBoolean())).sorted(keyOrder.on[Assignable](_.key)) shouldBe keyValues
      }
    }
  }
}
