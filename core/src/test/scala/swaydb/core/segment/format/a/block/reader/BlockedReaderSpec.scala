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
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.core.segment.format.a.block.reader

import org.scalamock.scalatest.MockFactory
import swaydb.core.CommonAssertions.orNone
import swaydb.core.TestData._
import swaydb.core.segment.format.a.block.segment.SegmentBlock
import swaydb.core.segment.format.a.block.values.ValuesBlock
import swaydb.core.segment.format.a.block.{Block, BlockCache}
import swaydb.core.{TestBase, TestCaseSweeper}
import swaydb.data.slice.Slice

class BlockedReaderSpec extends TestBase with MockFactory {

  "apply" when {
    "ref" in {
      val header = Slice(1.toByte, 0.toByte)
      val body = randomBytesSlice(100)
      val bytes = header ++ body

      val ref = BlockRefReader[ValuesBlock.Offset](bytes)
      BlockedReader(ref).readRemaining() shouldBe body
    }

    "unblocked Segment" in {
      TestCaseSweeper {
        implicit sweeper =>

          val blockCache = orNone(BlockCache.forSearch(sweeper.blockSweeperCache))

          implicit val ops = SegmentBlock.SegmentBlockOps

          val childHeader = Slice(1.toByte, 0.toByte)
          val childBody = Slice.fill(20)(9.toByte)
          val childBytes = childHeader ++ childBody

          val segmentHeader = Slice(1.toByte, 0.toByte)
          val segmentBody = childBytes
          val segmentBytes = segmentHeader ++ segmentBody

          val segmentRef = BlockRefReader[SegmentBlock.Offset](segmentBytes)
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
