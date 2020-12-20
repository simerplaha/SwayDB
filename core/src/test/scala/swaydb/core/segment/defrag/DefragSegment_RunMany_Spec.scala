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

class DefragSegment_RunMany_Spec extends TestBase with MockFactory with EitherValues {

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
}
