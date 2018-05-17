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

import scala.concurrent.duration._

class KeyValueMerger1_Update_Some_None_Into_Put_Spec extends WordSpec with Matchers with CommonAssertions {

  /**
    * Update(Some, None) -> Put(None, None)
    */

  "Update(Some, None) -> Put(None, None)" when {
    "Put(None, None)" in {
      (Memory.Update(1, 1, None), Memory.Put(1, None, None)).merge shouldBe Memory.Put(1, 1, None)
    }
  }

  /**
    * Update(Some, None) -> Put(None, Some)
    */

  "Update(Some, None) -> Put(None, Some)" when {
    "Put(None, Some)" in {
      (1 to 20) foreach {
        i =>
          //deadline for newKeyValues are not validated. HasTimeLeft, HasNoTimeLeft or Expired does have any logic for newKeyValues during merge.
          //the loop checks for all deadline conditions.
          val deadline = i.seconds.fromNow - 10.seconds //10.seconds to also account for expired deadlines.
          (Memory.Update(1, 1, deadline), Memory.Put(1, None, None)).merge shouldBe Memory.Put(1, 1, deadline)
      }
    }
  }

  /**
    * Update(Some, None) -> Put(Some, None)
    */

  "Update(Some, None) -> Put(Some, None)" when {
    "Put(Some, None)" in {
      (Memory.Update(1, 1, None), Memory.Put(1, "value", None)).merge shouldBe Memory.Put(1, 1, None)
    }
  }

  "Update(Some, None) -> Put(Some, Some)" when {
    "Put(Some, Some)" in {
      (1 to 20) foreach {
        i =>
          //deadline for newKeyValues are not validated. HasTimeLeft, HasNoTimeLeft or Expired does have any logic for newKeyValues during merge.
          //the loop checks for all deadline conditions.
          val deadline = i.seconds.fromNow - 10.seconds //10.seconds to also account for expired deadlines.
          (Memory.Update(1, 1, None), Memory.Put(1, None, deadline)).merge shouldBe Memory.Put(1, 1, deadline)
      }
    }
  }

}
