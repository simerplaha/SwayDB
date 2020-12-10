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

package swaydb.core.segment

import swaydb.core.CommonAssertions._
import swaydb.core.TestCaseSweeper._
import swaydb.core.TestData._
import swaydb.core.io.reader.Reader
import swaydb.core.segment.io.SegmentReadIO
import swaydb.core.{TestBase, TestCaseSweeper}
import swaydb.data.RunThis._
import swaydb.data.config.MMAP
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice

class SegmentSerialiserSpec extends TestBase {

  "serialise segment" in {
    runThis(100.times, log = true, "Test - Serialise segment") {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._

          implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default
          implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
          implicit val segmentIO: SegmentReadIO = SegmentReadIO.random

          val keyValues = randomizedKeyValues(randomIntMax(100) max 1)
          val segment = TestSegment(keyValues)

          val bytes = Slice.of[Byte](SegmentSerialiser.FormatA.bytesRequired(segment))

          SegmentSerialiser.FormatA.write(
            segment = segment,
            bytes = bytes
          )

          bytes.isOriginalFullSlice shouldBe true

          val readSegment =
            SegmentSerialiser.FormatA.read(
              reader = Reader(bytes),
              mmapSegment = MMAP.randomForSegment(),
              segmentRefCacheWeight = randomByte(),
              checkExists = segment.persistent,
              removeDeletes = false
            ).sweep()

          readSegment shouldBe segment
      }
    }
  }
}
