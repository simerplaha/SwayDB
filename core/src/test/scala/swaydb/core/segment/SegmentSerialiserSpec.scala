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

package swaydb.core.segment

import swaydb.config.MMAP
import swaydb.core.CommonAssertions._
import swaydb.core.TestCaseSweeper._
import swaydb.core.CoreTestData._
import swaydb.core.file.reader.Reader
import swaydb.core.segment.io.SegmentReadIO
import swaydb.core.{CoreTestBase, TestCaseSweeper}
import swaydb.slice.Slice
import swaydb.slice.order.{KeyOrder, TimeOrder}
import swaydb.testkit.RunThis._
import swaydb.testkit.TestKit._

class SegmentSerialiserSpec extends CoreTestBase {

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
              segmentRefCacheLife = randomSegmentRefCacheLife(),
              checkExists = segment.persistent
            ).sweep()

          readSegment shouldBe segment
      }
    }
  }
}
