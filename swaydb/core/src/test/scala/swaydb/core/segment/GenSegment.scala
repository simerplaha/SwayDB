/*
 * Copyright (c) 19/12/21, 5:43 pm Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package swaydb.core.segment

import org.scalatest.matchers.should.Matchers._
import swaydb.core.{CoreSpecType, CoreTestSweeper}
import swaydb.core.segment.block.binarysearch.BinarySearchIndexBlockConfig
import swaydb.core.segment.block.segment.SegmentBlockConfig
import swaydb.core.segment.block.values.ValuesBlockConfig
import swaydb.core.segment.SegmentTestKit.mmapSegments
import swaydb.core.segment.block.bloomfilter.BloomFilterBlockConfig
import swaydb.core.segment.block.hashindex.HashIndexBlockConfig
import swaydb.core.segment.data.{Memory, SegmentKeyOrders}
import swaydb.core.segment.io.SegmentReadIO
import swaydb.slice.order.{KeyOrder, TimeOrder}
import swaydb.TestExecutionContext
import swaydb.core.segment.block.sortedindex.SortedIndexBlockConfig
import swaydb.core.segment.data.merge.stats.MergeStats
import swaydb.core.CoreTestSweeper._
import swaydb.core.segment.block.SegmentBlockTestKit._
import swaydb.slice.Slice
import swaydb.testkit.TestKit.{eitherOne, randomIntMax}

import scala.concurrent.ExecutionContext

object GenSegment {
  def apply(keyValues: Slice[Memory],
            createdInLevel: Int = 1,
            valuesConfig: ValuesBlockConfig = ValuesBlockConfig.random,
            sortedIndexConfig: SortedIndexBlockConfig = SortedIndexBlockConfig.random,
            binarySearchIndexConfig: BinarySearchIndexBlockConfig = BinarySearchIndexBlockConfig.random,
            hashIndexConfig: HashIndexBlockConfig = HashIndexBlockConfig.random,
            bloomFilterConfig: BloomFilterBlockConfig = BloomFilterBlockConfig.random,
            segmentConfig: SegmentBlockConfig = SegmentBlockConfig.random.copy(mmap = mmapSegments))(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                                                                                     timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long,
                                                                                                     sweeper: CoreTestSweeper,
                                                                                                     coreSpecType: CoreSpecType,
                                                                                                     ec: ExecutionContext = TestExecutionContext.executionContext): Segment = {
    val segments =
      many(
        createdInLevel = createdInLevel,
        keyValues = keyValues,
        valuesConfig = valuesConfig,
        sortedIndexConfig = sortedIndexConfig,
        binarySearchIndexConfig = binarySearchIndexConfig,
        hashIndexConfig = hashIndexConfig,
        bloomFilterConfig = bloomFilterConfig,
        segmentConfig =
          if (coreSpecType.isPersistent)
            segmentConfig.copy(minSize = Int.MaxValue, maxCount = eitherOne(randomIntMax(keyValues.size), Int.MaxValue))
          else
            segmentConfig.copy(minSize = Int.MaxValue, maxCount = Int.MaxValue)
      )

    segments should have size 1

    segments.head
  }

  def one(keyValues: Slice[Memory],
          createdInLevel: Int = 1,
          valuesConfig: ValuesBlockConfig = ValuesBlockConfig.random,
          sortedIndexConfig: SortedIndexBlockConfig = SortedIndexBlockConfig.random,
          binarySearchIndexConfig: BinarySearchIndexBlockConfig = BinarySearchIndexBlockConfig.random,
          hashIndexConfig: HashIndexBlockConfig = HashIndexBlockConfig.random,
          bloomFilterConfig: BloomFilterBlockConfig = BloomFilterBlockConfig.random,
          segmentConfig: SegmentBlockConfig = SegmentBlockConfig.random.copy(mmap = mmapSegments))(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                                                                                   timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long,
                                                                                                   sweeper: CoreTestSweeper,
                                                                                                   coreSpecType: CoreSpecType,
                                                                                                   ec: ExecutionContext = TestExecutionContext.executionContext): Segment = {

    val segments =
      many(
        createdInLevel = createdInLevel,
        keyValues = keyValues,
        valuesConfig = valuesConfig,
        sortedIndexConfig = sortedIndexConfig,
        binarySearchIndexConfig = binarySearchIndexConfig,
        hashIndexConfig = hashIndexConfig,
        bloomFilterConfig = bloomFilterConfig,
        segmentConfig = segmentConfig.copy(minSize = Int.MaxValue, maxCount = Int.MaxValue)
      )

    segments should have size 1

    segments.head
  }

  def many(keyValues: Slice[Memory],
           createdInLevel: Int = 1,
           valuesConfig: ValuesBlockConfig = ValuesBlockConfig.random,
           sortedIndexConfig: SortedIndexBlockConfig = SortedIndexBlockConfig.random,
           binarySearchIndexConfig: BinarySearchIndexBlockConfig = BinarySearchIndexBlockConfig.random,
           hashIndexConfig: HashIndexBlockConfig = HashIndexBlockConfig.random,
           bloomFilterConfig: BloomFilterBlockConfig = BloomFilterBlockConfig.random,
           segmentConfig: SegmentBlockConfig = SegmentBlockConfig.random.copy(mmap = mmapSegments))(implicit keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                                                                                    timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long,
                                                                                                    sweeper: CoreTestSweeper,
                                                                                                    coreSpecType: CoreSpecType,
                                                                                                    ec: ExecutionContext = TestExecutionContext.executionContext): Slice[Segment] = {
    import swaydb.testkit.RunThis._
    import sweeper._

    implicit val segmentIO: SegmentReadIO =
      SegmentReadIO(
        bloomFilterConfig = bloomFilterConfig,
        hashIndexConfig = hashIndexConfig,
        binarySearchIndexConfig = binarySearchIndexConfig,
        sortedIndexConfig = sortedIndexConfig,
        valuesConfig = valuesConfig,
        segmentConfig = segmentConfig
      )

    implicit val segmentKeyOrders: SegmentKeyOrders =
      SegmentKeyOrders(keyOrder)

    val segment =
      if (coreSpecType.isMemory)
        MemorySegment(
          minSegmentSize = segmentConfig.minSize,
          maxKeyValueCountPerSegment = segmentConfig.maxCount,
          pathDistributor = sweeper.pathDistributor,
          createdInLevel = createdInLevel,
          stats = MergeStats.memoryBuilder(keyValues).close()
        )
      else
        PersistentSegment(
          pathDistributor = sweeper.pathDistributor,
          createdInLevel = createdInLevel,
          bloomFilterConfig = bloomFilterConfig,
          hashIndexConfig = hashIndexConfig,
          binarySearchIndexConfig = binarySearchIndexConfig,
          sortedIndexConfig = sortedIndexConfig,
          valuesConfig = valuesConfig,
          segmentConfig = segmentConfig,
          mergeStats =
            MergeStats
              .persistentBuilder(keyValues)
              .close(
                hasAccessPositionIndex = sortedIndexConfig.enableAccessPositionIndex,
                optimiseForReverseIteration = sortedIndexConfig.optimiseForReverseIteration
              )
        ).awaitInf

    segment.foreach(_.sweep())

    Slice.from(segment, segment.size)
  }
}
