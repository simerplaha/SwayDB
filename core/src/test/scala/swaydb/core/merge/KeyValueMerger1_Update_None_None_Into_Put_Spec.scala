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
import swaydb.core.data.KeyValue.ReadOnly
import swaydb.core.data.Memory
import swaydb.core.segment.KeyValueMerger
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.concurrent.duration._

class KeyValueMerger1_Update_None_None_Into_Put_Spec extends WordSpec with Matchers with CommonAssertions {

  /**
    * Update(None, None) -> Put(None, None)
    */

  "Update(None, None) -> Put(None, None)" when {
    "Put(None, None)" in {
      (Memory.Update(1, None, None), Memory.Put(1, None, None)).applyValue shouldBe Memory.Put(1, None, None)
    }
  }

  /**
    * Update(None, None) -> Put(None, Some)
    */

  "Update(None, None) -> Put(None, Some)" when {
    "Put(None, HasTimeLeft)" in {
      val deadline = 20.seconds.fromNow
      (Memory.Update(1, None, None), Memory.Put(1, None, deadline)).applyValue shouldBe Memory.Put(1, None, deadline)
    }

    "Put(None, HasNoTimeLeft)" in {
      val deadline = 5.seconds.fromNow
      (Memory.Update(1, None, None), Memory.Put(1, None, deadline)).applyValue shouldBe Memory.Put(1, None, deadline)
    }

    "Put(None, Expired)" in {
      val deadline = expiredDeadline()
      (Memory.Update(1, None, None), Memory.Put(1, None, deadline)).applyValue shouldBe Memory.Put(1, None, deadline)
    }
  }

  /**
    * Update(None, None) -> Put(Some, None)
    */

  "Update(None, None) -> Put(Some, None)" when {
    "Put(Some, None)" in {
      (Memory.Update(1, None, None), Memory.Put(1, "value", None)).applyValue shouldBe Memory.Put(1, None, None)
    }
  }

  "Update(None, None) -> Put(Some, Some)" when {
    "Put(None, HasTimeLeft)" in {
      val deadline = 20.seconds.fromNow
      (Memory.Update(1, None, None), Memory.Put(1, 1, deadline)).applyValue shouldBe Memory.Put(1, None, deadline)
    }

    "Put(None, HasNoTimeLeft)" in {
      val deadline = 5.seconds.fromNow
      (Memory.Update(1, None, None), Memory.Put(1, 1, deadline)).applyValue shouldBe Memory.Put(1, 1, deadline)
    }

    "Put(None, Expired)" in {
      val deadline = expiredDeadline()
      (Memory.Update(1, None, None), Memory.Put(1, 1, deadline)).applyValue shouldBe Memory.Put(1, 1, deadline)
    }
  }

}
