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

package swaydb.core.segment.block.reader

import org.scalamock.scalatest.MockFactory
import swaydb.core.CoreTestData._
import swaydb.core.segment.block.segment.{SegmentBlock, SegmentBlockOffset}
import swaydb.core.segment.block.values.ValuesBlockOffset
import swaydb.core.segment.block.{Block, BlockCache, BlockOps}
import swaydb.core.{ACoreSpec, TestCaseSweeper}
import swaydb.core.segment.ASegmentSpec
import swaydb.slice.Slice
import swaydb.testkit.RunThis._
import swaydb.testkit.TestKit._

class BlockedReaderSpec extends ASegmentSpec with MockFactory {

  "apply" when {
    "ref" in {
      runThis(20.times) {
        val header = Slice(1.toByte, 0.toByte)
        val body = randomBytesSlice(100)
        val bytes = header ++ body

        val ref = BlockRefReader[ValuesBlockOffset](bytes)
        val blockedReader = BlockedReader(ref)

        blockedReader.readRemaining() shouldBe body

        val unblockedReader = UnblockedReader(blockedReader, randomBoolean())
        unblockedReader.readAllAndGetReader().readFullBlock() shouldBe body
      }
    }

    "unblocked Segment" in {
      TestCaseSweeper {
        implicit sweeper =>

          val blockCache = orNone(BlockCache.forSearch(0, sweeper.blockSweeperCache))

          implicit val ops: BlockOps[SegmentBlockOffset, SegmentBlock] =
            SegmentBlockOffset.SegmentBlockOps

          val childHeader = Slice(1.toByte, 0.toByte)
          val childBody = Slice.fill(20)(9.toByte)
          val childBytes = childHeader ++ childBody

          val segmentHeader = Slice(1.toByte, 0.toByte)
          val segmentBody = childBytes
          val segmentBytes = segmentHeader ++ segmentBody

          val segmentRef = BlockRefReader[SegmentBlockOffset](segmentBytes)
          val segmentUnblocked = Block.unblock(segmentRef)
          segmentUnblocked.copy().readRemaining() shouldBe childBytes

          val childBlockRef = BlockRefReader.moveTo(0, childBytes.size, segmentUnblocked, blockCache)
          childBlockRef.copy().readRemaining() shouldBe childBytes
          val childUnblockedReader = Block.unblock(childBlockRef)

          childUnblockedReader.readRemaining() shouldBe childBody
      }
    }
  }
}
