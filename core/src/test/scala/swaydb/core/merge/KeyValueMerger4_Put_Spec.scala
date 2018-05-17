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

import org.scalatest.{Matchers, WordSpec}
import swaydb.core.CommonAssertions
import swaydb.core.data.Memory
import swaydb.serializers.Default._
import swaydb.serializers._

class KeyValueMerger4_Put_Spec extends WordSpec with Matchers with CommonAssertions {

  "Merging Put in any other randomly selected key-value" should {
    "always return the new put overwriting the old key-value" in {

      (1 to 1000) foreach {
        i =>
          val newKeyValue = Memory.Put(i, randomStringOption, randomDeadlineOption)
          val oldKeyValue = randomFixedKeyValue(i)

          (newKeyValue, oldKeyValue).merge shouldBe newKeyValue
      }
    }
  }
}
