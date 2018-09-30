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
import swaydb.core.function.FunctionStore
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.concurrent.duration._

class KeyValueMerger5_UpdateFunction_Some_Into_Update_Value_Spec extends WordSpec with Matchers with CommonAssertions {

  /**
    * UpdateFunction(Some) -> Update(None, None)
    */

  val incrementBy1FunctionId = randomCharacters()
  FunctionStore.put(incrementBy1FunctionId, inputBytes => inputBytes.readInt() + 1).assertGet shouldBe incrementBy1FunctionId

  "UpdateFunction(Some) -> Update(None, None)" when {
    "Update(None, None)" in {
      (1 to 20) foreach {
        i =>
          val deadline = i.seconds.fromNow - 2.seconds //-2.seconds to also account for expired deadlines.
          (Memory.UpdateFunction(1, incrementBy1FunctionId, deadline), Memory.Update(1, None, None)).mergeFailed.getMessage shouldBe "No old value specified"
      }
    }
  }

  /**
    * UpdateFunction(Some) -> Update(None, Some)
    */

  "UpdateFunction(Some) -> Update(None, Some)" when {
    "Update(None, HasTimeLeft-Greater)" in {
      val deadline = 30.seconds.fromNow
      val deadline2 = 20.seconds.fromNow
      (Memory.UpdateFunction(1, incrementBy1FunctionId, deadline), Memory.Update(1, None, deadline2)).mergeFailed.getMessage shouldBe "No old value specified"
    }

    "Update(None, HasTimeLeft-Lesser)" in {
      val deadline = 10.seconds.fromNow
      val deadline2 = 20.seconds.fromNow
      (Memory.UpdateFunction(1, incrementBy1FunctionId, deadline), Memory.Update(1, None, deadline2)).mergeFailed.getMessage shouldBe "No old value specified"
    }

    "Update(None, HasNoTimeLeft-Greater)" in {
      val deadline = 30.seconds.fromNow
      val deadline2 = 2.seconds.fromNow
      (Memory.UpdateFunction(1, incrementBy1FunctionId, deadline), Memory.Update(1, None, deadline2)).mergeFailed.getMessage shouldBe "No old value specified"
    }

    "Update(None, HasNoTimeLeft-Lesser)" in {
      val deadline = 1.seconds.fromNow
      val deadline2 = 2.seconds.fromNow
      (Memory.UpdateFunction(1, incrementBy1FunctionId, deadline), Memory.Update(1, None, deadline2)).mergeFailed.getMessage shouldBe "No old value specified"
    }

    "Update(None, Expired-Greater)" in {
      val deadline = 30.seconds.fromNow
      val deadline2 = expiredDeadline()
      (Memory.UpdateFunction(1, incrementBy1FunctionId, deadline), Memory.Update(1, None, deadline2)).mergeFailed.getMessage shouldBe "No old value specified"
    }

    "Update(None, Expired-Lesser)" in {
      val deadline2 = expiredDeadline()
      val deadline = deadline2 - 10.seconds
      (Memory.UpdateFunction(1, incrementBy1FunctionId, deadline), Memory.Update(1, None, deadline2)).mergeFailed.getMessage shouldBe "No old value specified"
    }
  }


  /**
    * UpdateFunction(Some) -> Update(Some, None)
    */

  "UpdateFunction(Some) -> Update(Some, None)" when {
    "Update(None, None)" in {
      (1 to 20) foreach {
        i =>
          //deadline for newKeyValues are not validated. HasTimeLeft, HasNoTimeLeft or Expired does have any logic for newKeyValues during merge.
          //the loop checks for all deadline conditions.
          val deadline = i.seconds.fromNow - 2.seconds //-2.seconds to also account for expired deadlines.
          (Memory.UpdateFunction(i, incrementBy1FunctionId, deadline), Memory.Update(i, i, None)).merge shouldBe Memory.Update(i, i + 1, deadline)
      }
    }
  }

  /**
    * UpdateFunction(Some) -> Update(Some, Some)
    */

  "UpdateFunction(Some) -> Update(Some, Some)" when {
    "Update(Some, HasTimeLeft-Greater)" in {
      val deadline = 30.seconds.fromNow
      val deadline2 = 20.seconds.fromNow
      (Memory.UpdateFunction(1, incrementBy1FunctionId, deadline), Memory.Update(1, 1, deadline2)).merge shouldBe Memory.Update(1, 2, deadline)
    }

    "Update(Some, HasTimeLeft-Lesser)" in {
      val deadline = 10.seconds.fromNow
      val deadline2 = 20.seconds.fromNow
      (Memory.UpdateFunction(1, incrementBy1FunctionId, deadline), Memory.Update(1, 1, deadline2)).merge shouldBe Memory.Update(1, 2, deadline)
    }

    "Update(Some, HasNoTimeLeft-Greater)" in {
      val deadline = 30.seconds.fromNow
      val deadline2 = 2.seconds.fromNow
      (Memory.UpdateFunction(1, incrementBy1FunctionId, deadline), Memory.Update(1, 1, deadline2)).merge shouldBe Memory.Update(1, 2, deadline2)
    }

    "Update(Some, HasNoTimeLeft-Lesser)" in {
      val deadline = 1.seconds.fromNow
      val deadline2 = 2.seconds.fromNow
      (Memory.UpdateFunction(1, incrementBy1FunctionId, deadline), Memory.Update(1, 1, deadline2)).merge shouldBe Memory.Update(1, 2, deadline)
    }

    "Update(Some, Expired-Greater)" in {
      val deadline = 30.seconds.fromNow
      val deadline2 = expiredDeadline()
      (Memory.UpdateFunction(1, incrementBy1FunctionId, deadline), Memory.Update(1, 1, deadline2)).merge shouldBe Memory.Update(1, 2, deadline2)
    }

    "Update(Some, Expired-Lesser)" in {
      val deadline2 = expiredDeadline()
      val deadline = deadline2 - 10.seconds
      (Memory.UpdateFunction(1, incrementBy1FunctionId, deadline), Memory.Update(1, 1, deadline2)).merge shouldBe Memory.Update(1, 2, deadline)
    }
  }

}
