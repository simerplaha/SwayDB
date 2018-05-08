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

class KeyValueMerger2_Update_Some_Some_Into_Remove_Spec extends WordSpec with Matchers with CommonAssertions {

  /**
    * Update(Some, Some) -> Remove(None)
    */

  "Update(Some, Some) -> Remove(None)" when {
    "Remove(None)" in {
      (1 to 20) foreach {
        i =>
          val deadline = i.seconds.fromNow - 2.seconds
          (Memory.Update(1, 1, deadline), Memory.Remove(1, None)).applyValue shouldBe Memory.Remove(1, None)

      }
    }
  }

  /**
    * Update(Some, Some) -> Remove(Some)
    */

  "Update(Some, Some) -> Remove(Some)" when {
    "Remove(HasTimeLeft-Greater)" in {
      val deadline = 30.seconds.fromNow
      val deadline2 = 20.seconds.fromNow
      (Memory.Update(1, 1, deadline), Memory.Remove(1, deadline2)).applyValue shouldBe Memory.Update(1, 1, deadline)
    }

    "Remove(HasTimeLeft-Lesser)" in {
      val deadline = 20.seconds.fromNow
      val deadline2 = 30.seconds.fromNow
      (Memory.Update(1, 1, deadline), Memory.Remove(1, deadline2)).applyValue shouldBe Memory.Update(1, 1, deadline)
    }

    "Remove(HasNoTimeLeft-Greater)" in {
      val deadline = 30.seconds.fromNow
      val deadline2 = 2.seconds.fromNow
      (Memory.Update(1, 1, deadline), Memory.Remove(1, deadline2)).applyValue shouldBe Memory.Remove(1, deadline2)
    }

    "Remove(HasNoTimeLeft-Lesser)" in {
      val deadline = 1.seconds.fromNow
      val deadline2 = 2.seconds.fromNow
      (Memory.Update(1, 1, deadline), Memory.Remove(1, deadline2)).applyValue shouldBe Memory.Update(1, 1, deadline)
    }

    "Remove(Expired-Greater)" in {
      val deadline = 30.seconds.fromNow
      val deadline2 = expiredDeadline()
      (Memory.Update(1, 1, deadline), Memory.Remove(1, deadline2)).applyValue shouldBe Memory.Remove(1, deadline2)
    }

    "Remove(Expired-Lesser)" in {
      val deadline2 = expiredDeadline()
      val deadline = deadline2 - 1.seconds
      (Memory.Update(1, 1, deadline), Memory.Remove(1, deadline2)).applyValue shouldBe Memory.Update(1, 1, deadline)
    }
  }
}
