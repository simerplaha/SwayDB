/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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

package swaydb.core.segment.merge

import org.scalatest.{Matchers, WordSpec}
import swaydb.core.TestData._
import swaydb.core.RunThis._
import swaydb.core.CommonAssertions._
import swaydb.serializers._
import swaydb.serializers.Default._

class SegmentBufferSpec extends WordSpec with Matchers {

  "apply" when {
    "flatten" in {
      SegmentBuffer(None).isInstanceOf[SegmentBuffer.Flattened] shouldBe true
    }

    "grouped" in {
      SegmentBuffer(Some(randomGroupBy(10))).isInstanceOf[SegmentBuffer.Grouped] shouldBe true
    }
  }

  "flatten" should {
    "add" in {
      val flattened = SegmentBuffer(None)
      val keyValue = randomFixedTransientKeyValue(1)
      flattened add keyValue
      flattened should have size 1
      flattened.head shouldBe keyValue
      flattened.isReadyForGrouping shouldBe false
    }
  }

  "grouped" when {
    "no groupByGroup" should {
      "add to ungrouped" in {
        val buffer = SegmentBuffer(Some(randomGroupBy(10, None))).asInstanceOf[SegmentBuffer.Grouped]

        buffer.groupedKeyValues.size shouldBe 0
        buffer.isReadyForGrouping shouldBe false
        buffer add randomFixedTransientKeyValue(1)
        buffer add randomFixedTransientKeyValue(2)

        buffer.size shouldBe 2
        buffer.isReadyForGrouping shouldBe false
        buffer.groupedKeyValues.size shouldBe 0
        //add 8 more to make it ready for grouping
        (3 to 10) foreach {
          i =>
            buffer add randomFixedTransientKeyValue(i)
        }
        assertThrows[ArrayIndexOutOfBoundsException] {
          buffer add randomFixedTransientKeyValue(100)
        }
        buffer.isReadyForGrouping shouldBe true
        buffer.shouldGroupKeyValues(false) shouldBe true

        //adding a group when it has ungrouped key-value should fail
        buffer.addGroup[Throwable](randomGroup()).failed.get.getMessage.contains("unGrouped") shouldBe true

        //replaceGroupedKeyValues clear key-values
        buffer replaceGroupedKeyValues randomGroup()
        buffer.unGrouped.size shouldBe 0
        buffer.currentGroups.size shouldBe 1

        (1 to 10) foreach {
          i =>
            buffer add randomFixedTransientKeyValue(i)
        }

        buffer replaceGroupedKeyValues randomGroup()
        buffer.unGrouped.size shouldBe 0
        buffer.currentGroups.size shouldBe 2

        buffer replaceGroupedGroups randomGroup()
        buffer.unGrouped.size shouldBe 0
        buffer.currentGroups.size shouldBe 1
      }
    }
  }
}
