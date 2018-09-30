/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.segment.merge

import swaydb.core.TestBase
import swaydb.core.data._
import swaydb.data.util.StorageUnits._
import swaydb.order.KeyOrder
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.collection.mutable.ListBuffer

class SegmentGrouperSpec extends TestBase {

  override implicit val ordering = KeyOrder.default
  implicit val compression = groupingStrategy
  val keyValueCount = 100

  import ordering._

  "SegmentGrouper.addKeyValue" should {
    "add KeyValue to next split and close the split if the new key-value does not fit" in {

      val initialSegment = ListBuffer[KeyValue.WriteOnly]()
      initialSegment += Transient.Put(key = 1, value = 1, previous = None, falsePositiveRate = 0.1, compressDuplicateValues = true)
      initialSegment += Transient.Put(key = 2, value = 2, previous = initialSegment.lastOption, falsePositiveRate = 0.1, compressDuplicateValues = true) //total segmentSize is 60 bytes

      val segments = ListBuffer[ListBuffer[KeyValue.WriteOnly]](initialSegment)
      //this KeyValue's segment size without footer is 13 bytes
      val keyValue = Memory.Put(3, 3)

      //minSegmentSize is 69.bytes. Adding the above keyValues should create a segment of total size
      // 60 + 13 - 3 (common bytes between 2 and 3) which is over the limit = 70.bytes.
      // this should result is closing the existing segment and starting a new segment
      SegmentGrouper.addKeyValue(keyValue, segments, 69.bytes, forInMemory = false, bloomFilterFalsePositiveRate = 0.1, isLastLevel = false, compressDuplicateValues = true)

      //the initialSegment should be closed and a new segment should get started
      segments.size shouldBe 2

      val firstSegment = segments.head
      firstSegment(0).key equiv initialSegment.head.key
      firstSegment(1).key equiv initialSegment.last.key
      firstSegment.last.stats.segmentSize shouldBe 69.bytes

      val secondSegment = segments.last
      secondSegment.head.key equiv keyValue.key
    }
  }
}
