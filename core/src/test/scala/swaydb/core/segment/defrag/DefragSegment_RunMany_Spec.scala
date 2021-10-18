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
import swaydb.core.level.PathsDistributor
import swaydb.core.segment.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.block.bloomfilter.BloomFilterBlock
import swaydb.core.segment.block.hashindex.HashIndexBlock
import swaydb.core.segment.block.segment.SegmentBlock
import swaydb.core.segment.block.sortedindex.SortedIndexBlock
import swaydb.core.segment.block.values.ValuesBlock
import swaydb.core.segment.io.SegmentReadIO
import swaydb.core.segment.{PersistentSegment, PersistentSegmentMany}
import swaydb.core.{TestBase, TestCaseSweeper, TestExecutionContext, TestTimer}
import swaydb.data.compaction.CompactionConfig.CompactionParallelism
import swaydb.data.config.MMAP
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.testkit.RunThis._

import scala.collection.mutable.ListBuffer
import scala.util.Random

class DefragSegment_RunMany_Spec extends TestBase with MockFactory with EitherValues {

  implicit val ec = TestExecutionContext.executionContext
  implicit val timer = TestTimer.Empty

  implicit val keyOrder = KeyOrder.default
  implicit val timerOrder = TimeOrder.long
  implicit def segmentReadIO = SegmentReadIO.random
  implicit val compactionParallelism = CompactionParallelism.availableProcessors()

  "NO GAP - empty should result in empty" in {
    runThis(20.times, log = true) {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._

          //HEAD - EMPTY
          //MID  - EMPTY
          //GAP  - EMPTY

          //SEG  - [0 - 99]

          implicit val valuesConfig: ValuesBlock.Config = ValuesBlock.Config.random
          implicit val sortedIndexConfig: SortedIndexBlock.Config = SortedIndexBlock.Config.random
          implicit val binarySearchIndexConfig: BinarySearchIndexBlock.Config = BinarySearchIndexBlock.Config.random
          implicit val hashIndexConfig: HashIndexBlock.Config = HashIndexBlock.Config.random
          implicit val bloomFilterConfig: BloomFilterBlock.Config = BloomFilterBlock.Config.random
          implicit val segmentConfig: SegmentBlock.Config = SegmentBlock.Config.random.copy(maxCount = 10, minSize = Int.MaxValue)
          implicit val pathsDistributor: PathsDistributor = createPathDistributor

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
              pathsDistributor = createPathDistributor,
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
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._

          //HEAD - EMPTY
          //MID  - [0 - 99]
          //GAP  - EMPTY

          //SEG  - [0 - 99]

          implicit val valuesConfig: ValuesBlock.Config = ValuesBlock.Config.random
          implicit val sortedIndexConfig: SortedIndexBlock.Config = SortedIndexBlock.Config.random
          implicit val binarySearchIndexConfig: BinarySearchIndexBlock.Config = BinarySearchIndexBlock.Config.random
          implicit val hashIndexConfig: HashIndexBlock.Config = HashIndexBlock.Config.random
          implicit val bloomFilterConfig: BloomFilterBlock.Config = BloomFilterBlock.Config.random
          implicit val segmentConfig: SegmentBlock.Config = SegmentBlock.Config.random.copy(maxCount = 10, minSize = Int.MaxValue)
          implicit val pathsDistributor: PathsDistributor = createPathDistributor

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
              pathsDistributor = createPathDistributor,
              segmentRefCacheLife = randomSegmentRefCacheLife(),
              mmap = MMAP.randomForSegment()
            ).awaitInf

          mergeResult.input shouldBe many
          mergeResult.output.flatMap(_.iterator(randomBoolean()))(keyOrder.on(_.key)) shouldBe keyValues
      }
    }
  }
}
