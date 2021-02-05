/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
