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

import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec
import swaydb.config.CoreConfigTestKit._
import swaydb.config.MMAP
import swaydb.core.{CoreSpecType, CoreTestSweeper}
import swaydb.core.CoreTestSweeper._
import swaydb.core.segment.data.KeyValueTestKit._
import swaydb.core.segment.data.SegmentKeyOrders
import swaydb.core.segment.io.SegmentReadIO
import swaydb.core.segment.SegmentTestKit._
import swaydb.slice.{Slice, SliceReader}
import swaydb.slice.order.{KeyOrder, TimeOrder}
import swaydb.testkit.RunThis._
import swaydb.testkit.TestKit._

class SegmentSerialiserSpec extends AnyWordSpec {

  "serialise segment" in {
    runThis(100.times, log = true, "Test - Serialise segment") {
      CoreTestSweeper {
        implicit sweeper =>
          import sweeper._

          implicit val coreSpecType: CoreSpecType = CoreSpecType.random()
          implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default
          implicit val keyOrders: SegmentKeyOrders = SegmentKeyOrders(keyOrder)
          implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
          implicit val segmentIO: SegmentReadIO = SegmentReadIO.random

          val keyValues = randomizedKeyValues(randomIntMax(100) max 1)
          val segment = GenSegment(keyValues)

          val bytes = Slice.allocate[Byte](SegmentSerialiser.FormatA.bytesRequired(segment))

          SegmentSerialiser.FormatA.write(
            segment = segment,
            bytes = bytes
          )

          bytes.isOriginalFullSlice shouldBe true

          val readSegment =
            SegmentSerialiser.FormatA.read(
              reader = SliceReader(bytes),
              mmapSegment = MMAP.randomForSegment(),
              segmentRefCacheLife = randomSegmentRefCacheLife(),
              checkExists = segment.persistent
            ).sweep()

          readSegment shouldBe segment
      }
    }
  }
}
