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

package swaydb.core.merge

import org.scalatest.WordSpec
import swaydb.core.CommonAssertions
import swaydb.core.data.Memory
import swaydb.order.KeyOrder
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.concurrent.duration._

class SegmentMerger5_Put_Spec extends WordSpec with CommonAssertions {

  override implicit val ordering = KeyOrder.default
  implicit val compression = groupingStrategy

  "Merging Put into any existing key-value" should {
    "always return the new Put key-value" in {
      (1 to 1000) foreach {
        key =>
          val put = Memory.Put(key, randomStringOption, randomDeadlineOption)
          assertMerge(
            newKeyValue = put,
            oldKeyValue = randomFixedKeyValue(key),
            expected = put,
            lastLevelExpect = if (put.hasTimeLeft()) Some(put) else None,
            hasTimeLeftAtLeast = key.seconds - 10.seconds
          )
      }
    }
  }
}
