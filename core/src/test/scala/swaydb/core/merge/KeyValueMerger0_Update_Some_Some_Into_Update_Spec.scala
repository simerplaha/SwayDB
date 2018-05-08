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

class KeyValueMerger0_Update_Some_Some_Into_Update_Spec extends WordSpec with Matchers with CommonAssertions {

  /**
    * Update(Some, Some) -> Update(None, None)
    */

  "Update(Some, Some) -> Update(None, None)" when {
    "Update(None, None)" in {
      (1 to 20) foreach {
        i =>
          val deadline = i.seconds.fromNow - 2.seconds
          (Memory.Update(1, 1, deadline), Memory.Update(1, None, None)).applyValue shouldBe Memory.Update(1, 1, deadline)
      }
    }
  }

  /**
    * Update(Some, Some) -> Update(None, Some)
    */

  "Update(Some, Some) -> Update(None, Some)" when {
    "Update(None, HasTimeLeft-Greater)" in {
      val deadline = 30.seconds.fromNow
      val deadline2 = 20.seconds.fromNow
      (Memory.Update(1, 1, deadline), Memory.Update(1, None, deadline2)).applyValue shouldBe Memory.Update(1, 1, deadline)
    }

    "Update(None, HasTimeLeft-Lesser)" in {
      val deadline = 10.seconds.fromNow
      val deadline2 = 20.seconds.fromNow
      (Memory.Update(1, 1, deadline), Memory.Update(1, None, deadline2)).applyValue shouldBe Memory.Update(1, 1, deadline)
    }

    "Update(None, HasNoTimeLeft-Greater)" in {
      val deadline = 30.seconds.fromNow
      val deadline2 = 2.seconds.fromNow
      (Memory.Update(1, 1, deadline), Memory.Update(1, None, deadline2)).applyValue shouldBe Memory.Update(1, None, deadline2)
    }

    "Update(None, HasNoTimeLeft-Lesser)" in {
      val deadline = 1.seconds.fromNow
      val deadline2 = 2.seconds.fromNow
      (Memory.Update(1, 1, deadline), Memory.Update(1, None, deadline2)).applyValue shouldBe Memory.Update(1, 1, deadline)
    }

    "Update(None, Expired-Greater)" in {
      val deadline = 30.seconds.fromNow
      val deadline2 = expiredDeadline()
      (Memory.Update(1, 1, deadline), Memory.Update(1, None, deadline2)).applyValue shouldBe Memory.Update(1, None, deadline2)
    }

    "Update(None, Expired-Lesser)" in {
      val deadline2 = expiredDeadline()
      val deadline = deadline2 - 1.seconds
      (Memory.Update(1, 1, deadline), Memory.Update(1, None, deadline2)).applyValue shouldBe Memory.Update(1, 1, deadline)
    }
  }

  /**
    * Update(Some, Some) -> Update(Some, None)
    */

  "Update(Some, Some) -> Update(Some, None)" when {
    "Update(Some, None)" in {
      (1 to 20) foreach {
        i =>
          val deadline = i.seconds.fromNow - 2.seconds
          (Memory.Update(1, 1, deadline), Memory.Update(1, "old value", None)).applyValue shouldBe Memory.Update(1, 1, deadline)
      }
    }
  }

  /**
    * Update(Some, Some) -> Update(Some, Some)
    */

  "Update(Some, Some) -> Update(Some, Some)" when {
    "Update(Some, HasTimeLeft-Greater)" in {
      val deadline = 30.seconds.fromNow
      val deadline2 = 20.seconds.fromNow
      (Memory.Update(1, 1, deadline), Memory.Update(1, 2, deadline2)).applyValue shouldBe Memory.Update(1, 1, deadline)
    }

    "Update(Some, HasTimeLeft-Lesser)" in {
      val deadline = 10.seconds.fromNow
      val deadline2 = 20.seconds.fromNow
      (Memory.Update(1, 1, deadline), Memory.Update(1, 2, deadline2)).applyValue shouldBe Memory.Update(1, 1, deadline)
    }

    "Update(Some, HasNoTimeLeft-Greater)" in {
      val deadline = 30.seconds.fromNow
      val deadline2 = 2.seconds.fromNow
      (Memory.Update(1, 1, deadline), Memory.Update(1, 2, deadline2)).applyValue shouldBe Memory.Update(1, 2, deadline2)
    }

    "Update(Some, HasNoTimeLeft-Lesser)" in {
      val deadline = 1.seconds.fromNow
      val deadline2 = 2.seconds.fromNow
      (Memory.Update(1, 1, deadline), Memory.Update(1, 2, deadline2)).applyValue shouldBe Memory.Update(1, 1, deadline)
    }

    "Update(Some, Expired-Greater)" in {
      val deadline = 30.seconds.fromNow
      val deadline2 = expiredDeadline()
      (Memory.Update(1, 1, deadline), Memory.Update(1, 2, deadline2)).applyValue shouldBe Memory.Update(1, 2, deadline2)
    }

    "Update(Some, Expired-Lesser)" in {
      val deadline2 = expiredDeadline()
      val deadline = deadline2 - 1.seconds
      (Memory.Update(1, 1, deadline), Memory.Update(1, 2, deadline2)).applyValue shouldBe Memory.Update(1, 1, deadline)
    }
  }
}
