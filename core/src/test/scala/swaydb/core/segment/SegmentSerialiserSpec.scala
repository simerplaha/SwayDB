/*
 * Copyright (c) 2020 Simer Plaha (@simerplaha)
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
 */

package swaydb.core.segment

import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.actor.{FileSweeper, MemorySweeper}
import swaydb.core.io.file.BlockCache
import swaydb.core.io.reader.Reader
import swaydb.core.{TestBase, TestSweeper}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice

class SegmentSerialiserSpec extends TestBase {

  "serialise segment" in {
    runThis(100.times, log = true, "Test - Serialise segment") {

      implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default
      implicit val keyValueMemorySweeper: Option[MemorySweeper.KeyValue] = TestSweeper.memorySweeperMax
      implicit val fileSweeper: FileSweeper.Enabled = TestSweeper.fileSweeper
      implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
      implicit val blockCache: Option[BlockCache.State] = TestSweeper.randomBlockCache
      implicit val segmentIO: SegmentIO = SegmentIO.random

      val keyValues = randomizedKeyValues(randomIntMax(100) max 1)
      val segment = TestSegment(keyValues)

      val bytes = Slice.create[Byte](SegmentSerialiser.FormatA.bytesRequired(segment))

      SegmentSerialiser.FormatA.write(
        segment = segment,
        bytes = bytes
      )

      bytes.isOriginalFullSlice shouldBe true

      val readSegment =
        SegmentSerialiser.FormatA.read(
          reader = Reader(bytes),
          mmapSegmentsOnRead = randomBoolean(),
          mmapSegmentsOnWrite = randomBoolean(),
          checkExists = segment.persistent
        )

      readSegment shouldBe segment

      segment.close
    }
  }
}
