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

class KeyValueMerger4_Remove_Some_Into_Update_Value_Spec extends WordSpec with Matchers with CommonAssertions {

  /**
    * Remove(Some) -> Update(None, None)
    */

  "Remove(Some) -> Update(None, None)" when {
    "Update(None, None)" in {
      val deadline = 20.seconds.fromNow

      (Memory.Remove(1, deadline), Memory.Update(1, None, None)).merge shouldBe Memory.Update(1, None, deadline)
    }
  }

  /**
    * Remove(Some) -> Update(None, Some)
    */

  "Remove(Some) -> Update(None, Some)" when {
    "Update(None, HasTimeLeft-Greater)" in {
      val deadline = 20.seconds.fromNow
      val deadline2 = 15.seconds.fromNow

      (Memory.Remove(1, deadline), Memory.Update(1, None, deadline2)).merge shouldBe Memory.Update(1, None, deadline)
    }

    "Update(None, HasTimeLeft-Lesser)" in {
      val deadline = 10.seconds.fromNow
      val deadline2 = 15.seconds.fromNow

      (Memory.Remove(1, deadline), Memory.Update(1, None, deadline2)).merge shouldBe Memory.Update(1, None, deadline)
    }

    "Update(None, HasTimeLeft-Expired)" in {
      val deadline = expiredDeadline()
      val deadline2 = 15.seconds.fromNow

      (Memory.Remove(1, deadline), Memory.Update(1, None, deadline2)).merge shouldBe Memory.Update(1, None, deadline)
    }

    "Update(None, HasNoTimeLeft-Greater)" in {
      val deadline = 20.seconds.fromNow
      val deadline2 = 1.seconds.fromNow

      (Memory.Remove(1, deadline), Memory.Update(1, None, deadline2)).merge shouldBe Memory.Update(1, None, deadline2)
    }

    "Update(None, HasNoTimeLeft-Lesser)" in {
      val deadline = 1.seconds.fromNow
      val deadline2 = 2.seconds.fromNow

      (Memory.Remove(1, deadline), Memory.Update(1, None, deadline2)).merge shouldBe Memory.Update(1, None, deadline)
    }

    "Update(None, Expired-Greater)" in {
      val deadline = 1.seconds.fromNow
      val deadline2 = expiredDeadline()

      (Memory.Remove(1, deadline), Memory.Update(1, None, deadline2)).merge shouldBe Memory.Update(1, None, deadline2)
    }

    "Update(None, Expired-Lesser)" in {
      val deadline2 = expiredDeadline()
      val deadline = deadline2 - 1.second

      (Memory.Remove(1, deadline), Memory.Update(1, None, deadline2)).merge shouldBe Memory.Update(1, None, deadline)
    }
  }

  /**
    * Remove(Some) -> Update(Some, None)
    */

  "Remove(Some) -> Update(Some, None)" when {
    "Update(None, None)" in {
      val deadline = 20.seconds.fromNow

      (Memory.Remove(1, deadline), Memory.Update(1, 1, None)).merge shouldBe Memory.Update(1, 1, deadline)
    }
  }

  /**
    * Remove(Some) -> Update(Some, Some)
    */

  "Remove(Some) -> Update(Some, Some)" when {
    "Update(Some, HasTimeLeft-Greater)" in {
      val deadline = 20.seconds.fromNow
      val deadline2 = 15.seconds.fromNow

      (Memory.Remove(1, deadline), Memory.Update(1, 1, deadline2)).merge shouldBe Memory.Update(1, 1, deadline)
    }

    "Update(None, HasTimeLeft-Lesser)" in {
      val deadline = 10.seconds.fromNow
      val deadline2 = 15.seconds.fromNow

      (Memory.Remove(1, deadline), Memory.Update(1, 1, deadline2)).merge shouldBe Memory.Update(1, 1, deadline)
    }

    "Update(None, HasTimeLeft-Expired)" in {
      val deadline = expiredDeadline()
      val deadline2 = 15.seconds.fromNow

      (Memory.Remove(1, deadline), Memory.Update(1, 1, deadline2)).merge shouldBe Memory.Update(1, 1, deadline)
    }

    "Update(None, HasNoTimeLeft-Greater)" in {
      val deadline = 20.seconds.fromNow
      val deadline2 = 1.seconds.fromNow

      (Memory.Remove(1, deadline), Memory.Update(1, 1, deadline2)).merge shouldBe Memory.Update(1, 1, deadline2)
    }

    "Update(None, HasNoTimeLeft-Lesser)" in {
      val deadline = 1.seconds.fromNow
      val deadline2 = 2.seconds.fromNow

      (Memory.Remove(1, deadline), Memory.Update(1, 1, deadline2)).merge shouldBe Memory.Update(1, 1, deadline)
    }

    "Update(None, Expired-Greater)" in {
      val deadline = 1.seconds.fromNow
      val deadline2 = expiredDeadline()

      (Memory.Remove(1, deadline), Memory.Update(1, 1, deadline2)).merge shouldBe Memory.Update(1, 1, deadline2)
    }

    "Update(None, Expired-Lesser)" in {
      val deadline2 = expiredDeadline()
      val deadline = deadline2 - 1.second

      (Memory.Remove(1, deadline), Memory.Update(1, 1, deadline2)).merge shouldBe Memory.Update(1, 1, deadline)
    }
  }

}
