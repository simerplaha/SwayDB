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
import swaydb.core.CommonAssertions._
import swaydb.core.TestData._
import swaydb.serializers.Default._
import swaydb.serializers._

class SegmentBufferSpec extends WordSpec with Matchers {

  "apply" when {
    "flatten" in {
      SegmentBuffer().isInstanceOf[SegmentBuffer.Flattened] shouldBe true
    }
  }

  "flatten" should {
    "add" in {
      val flattened = SegmentBuffer()
      val keyValue = randomFixedTransientKeyValue(1)
      flattened add keyValue
      flattened should have size 1
      flattened.head shouldBe keyValue
    }
  }
}
