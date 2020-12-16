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
import swaydb.core.TestData._
import swaydb.core.data.Memory
import swaydb.core.merge.MergeStats
import swaydb.core.segment.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.block.bloomfilter.BloomFilterBlock
import swaydb.core.segment.block.hashindex.HashIndexBlock
import swaydb.core.segment.block.segment.SegmentBlock
import swaydb.core.segment.block.segment.data.TransientSegment
import swaydb.core.segment.block.sortedindex.SortedIndexBlock
import swaydb.core.segment.block.values.ValuesBlock
import swaydb.core.{TestBase, TestCaseSweeper, TestExecutionContext}
import swaydb.data.util.StorageUnits._

import scala.collection.mutable.ListBuffer

class DefragGapSpec extends TestBase with MockFactory with EitherValues {

  implicit val ec = TestExecutionContext.executionContext

  "add Segments without opening" when {
    "there is no head MergeStats and removeDeletes is false" in {
      TestCaseSweeper {
        implicit sweeper =>

          implicit val valuesConfig: ValuesBlock.Config = ValuesBlock.Config.random
          implicit val sortedIndexConfig: SortedIndexBlock.Config = SortedIndexBlock.Config.random
          implicit val binarySearchIndexConfig: BinarySearchIndexBlock.Config = BinarySearchIndexBlock.Config.random
          implicit val hashIndexConfig: HashIndexBlock.Config = HashIndexBlock.Config.random
          implicit val bloomFilterConfig: BloomFilterBlock.Config = BloomFilterBlock.Config.random
          implicit val segmentConfig: SegmentBlock.Config = SegmentBlock.Config.random

          val segments = ListBuffer(1, 5).map(_ => TestSegment())

          val resultBuffer =
            DefragGap.run(
              gap = segments,
              fragments = ListBuffer.empty,
              removeDeletes = false,
              createdInLevel = 1
            )

          val expected =
            segments map {
              segment =>
                TransientSegment.RemoteSegment(
                  segment = segment,
                  removeDeletes = false,
                  createdInLevel = 1,
                  valuesConfig = valuesConfig,
                  sortedIndexConfig = sortedIndexConfig,
                  binarySearchIndexConfig = binarySearchIndexConfig,
                  hashIndexConfig = hashIndexConfig,
                  bloomFilterConfig = bloomFilterConfig,
                  segmentConfig = segmentConfig
                )
            }

          resultBuffer shouldBe expected
      }
    }

    "there is head MergeStats but it is > segmentConfig.minSize" in {
      TestCaseSweeper {
        implicit sweeper =>

          implicit val valuesConfig: ValuesBlock.Config = ValuesBlock.Config.random
          implicit val sortedIndexConfig: SortedIndexBlock.Config = SortedIndexBlock.Config.random
          implicit val binarySearchIndexConfig: BinarySearchIndexBlock.Config = BinarySearchIndexBlock.Config.random
          implicit val hashIndexConfig: HashIndexBlock.Config = HashIndexBlock.Config.random
          implicit val bloomFilterConfig: BloomFilterBlock.Config = BloomFilterBlock.Config.random
          //small minSize so that the size of head key-values is always considered large.
          implicit val segmentConfig: SegmentBlock.Config = SegmentBlock.Config.random.copy(minSize = 1.byte)

          val headKeyValues = TransientSegment.Stats(MergeStats.persistentBuilder[Memory](randomKeyValues()))
          val existingBuffer = ListBuffer[TransientSegment.Fragment](headKeyValues)

          val segments = ListBuffer(1, 5).map(_ => TestSegment())

          val resultBuffer =
            DefragGap.run(
              gap = segments,
              fragments = existingBuffer,
              removeDeletes = false,
              createdInLevel = 1
            )

          val expected =
            segments map {
              segment =>
                TransientSegment.RemoteSegment(
                  segment = segment,
                  removeDeletes = false,
                  createdInLevel = 1,
                  valuesConfig = valuesConfig,
                  sortedIndexConfig = sortedIndexConfig,
                  binarySearchIndexConfig = binarySearchIndexConfig,
                  hashIndexConfig = hashIndexConfig,
                  bloomFilterConfig = bloomFilterConfig,
                  segmentConfig = segmentConfig
                )
            }

          //expect the key-values and segments to get added
          resultBuffer should have size (segments.size + 1)

          //contains head key-values
          resultBuffer.head shouldBe headKeyValues
          //contain all the Segments
          resultBuffer.drop(1) shouldBe expected
      }
    }
  }
}
