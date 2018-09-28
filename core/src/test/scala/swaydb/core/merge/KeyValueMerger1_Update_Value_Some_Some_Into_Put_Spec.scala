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

class KeyValueMerger1_Update_Value_Some_Some_Into_Put_Spec extends WordSpec with Matchers with CommonAssertions {

  /**
    * Update(Some, Some) -> Put(None, None)
    */

  "Update(Some, Some) -> Put(None, None)" when {
    "Put(None, None)" in {
      (1 to 20) foreach {
        i =>
          val deadline = i.seconds.fromNow - 10.seconds
          (Memory.Update(1, 1, deadline), Memory.Put(1, None, None)).merge shouldBe Memory.Put(1, 1, deadline)
      }
    }
  }

  /**
    * Update(Some, Some) -> Put(None, Some)
    */

  "Update(Some, Some) -> Put(None, Some)" when {
    "Put(None, HasTimeLeft-Greater)" in {
      val deadline = 30.seconds.fromNow
      val deadline2 = 20.seconds.fromNow
      (Memory.Update(1, 1, deadline), Memory.Put(1, None, deadline2)).merge shouldBe Memory.Put(1, 1, deadline)
    }

    "Put(None, HasTimeLeft-Lesser)" in {
      val deadline = 10.seconds.fromNow
      val deadline2 = 20.seconds.fromNow
      (Memory.Update(1, 1, deadline), Memory.Put(1, None, deadline2)).merge shouldBe Memory.Put(1, 1, deadline)
    }

    "Put(None, HasNoTimeLeft-Greater)" in {
      val deadline = 30.seconds.fromNow
      val deadline2 = 2.seconds.fromNow
      (Memory.Update(1, 1, deadline), Memory.Put(1, None, deadline2)).merge shouldBe Memory.Put(1, 1, deadline2)
    }

    "Put(None, HasNoTimeLeft-Lesser)" in {
      val deadline = 1.seconds.fromNow
      val deadline2 = 2.seconds.fromNow
      (Memory.Update(1, 1, deadline), Memory.Put(1, None, deadline2)).merge shouldBe Memory.Put(1, 1, deadline)
    }

    "Put(None, Expired-Greater)" in {
      val deadline = 30.seconds.fromNow
      val deadline2 = expiredDeadline()
      (Memory.Update(1, 1, deadline), Memory.Put(1, None, deadline2)).merge shouldBe Memory.Put(1, 1, deadline2)
    }

    "Put(None, Expired-Lesser)" in {
      val deadline2 = expiredDeadline()
      val deadline = deadline2 - 1.seconds
      (Memory.Update(1, 1, deadline), Memory.Put(1, None, deadline2)).merge shouldBe Memory.Put(1, 1, deadline)
    }
  }

  /**
    * Update(Some, Some) -> Put(Some, None)
    */

  "Update(Some, Some) -> Put(Some, None)" when {
    "Put(Some, None)" in {
      (1 to 20) foreach {
        i =>
          val deadline = i.seconds.fromNow - 2.seconds
          (Memory.Update(1, 1, deadline), Memory.Put(1, "old value", None)).merge shouldBe Memory.Put(1, 1, deadline)
      }
    }
  }

  /**
    * Update(Some, Some) -> Put(Some, Some)
    */

  "Update(Some, Some) -> Put(Some, Some)" when {
    "Put(Some, HasTimeLeft-Greater)" in {
      val deadline = 30.seconds.fromNow
      val deadline2 = 20.seconds.fromNow
      (Memory.Update(1, 1, deadline), Memory.Put(1, 2, deadline2)).merge shouldBe Memory.Put(1, 1, deadline)
    }

    "Put(Some, HasTimeLeft-Lesser)" in {
      val deadline = 10.seconds.fromNow
      val deadline2 = 20.seconds.fromNow
      (Memory.Update(1, 1, deadline), Memory.Put(1, 2, deadline2)).merge shouldBe Memory.Put(1, 1, deadline)
    }

    "Put(Some, HasNoTimeLeft-Greater)" in {
      val deadline = 30.seconds.fromNow
      val deadline2 = 2.seconds.fromNow
      (Memory.Update(1, 1, deadline), Memory.Put(1, 2, deadline2)).merge shouldBe Memory.Put(1, 1, deadline2)
    }

    "Put(Some, HasNoTimeLeft-Lesser)" in {
      val deadline = 1.seconds.fromNow
      val deadline2 = 2.seconds.fromNow
      (Memory.Update(1, 1, deadline), Memory.Put(1, 2, deadline2)).merge shouldBe Memory.Put(1, 1, deadline)
    }

    "Put(Some, Expired-Greater)" in {
      val deadline = 30.seconds.fromNow
      val deadline2 = expiredDeadline()
      (Memory.Update(1, 1, deadline), Memory.Put(1, 2, deadline2)).merge shouldBe Memory.Put(1, 1, deadline2)
    }

    "Put(Some, Expired-Lesser)" in {
      val deadline2 = expiredDeadline()
      val deadline = deadline2 - 1.seconds
      (Memory.Update(1, 1, deadline), Memory.Put(1, 2, deadline2)).merge shouldBe Memory.Put(1, 1, deadline)
    }
  }
}
