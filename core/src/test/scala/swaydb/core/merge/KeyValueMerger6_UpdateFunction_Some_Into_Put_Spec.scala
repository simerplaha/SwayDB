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

class KeyValueMerger6_UpdateFunction_Some_Into_Put_Spec extends WordSpec with Matchers with CommonAssertions {

  /**
    * UpdateFunction(Some) -> Put(None, None)
    */

  val incrementBy1FunctionId = randomCharacters()
  FunctionStore.put(incrementBy1FunctionId, inputBytes => inputBytes.readInt() + 1).assertGet shouldBe incrementBy1FunctionId

  "UpdateFunction(Some) -> Put(None, None)" when {
    "Put(None, None)" in {
      (1 to 20) foreach {
        i =>
          //deadline for newKeyValues are not validated. HasTimeLeft, HasNoTimeLeft or Expired does have any logic for newKeyValues during merge.
          //the loop checks for all deadline conditions.
          val deadline = i.seconds.fromNow - 10.seconds //-10.seconds to also account for expired deadlines.
          (Memory.UpdateFunction(1, incrementBy1FunctionId, deadline), Memory.Put(1, None, None)).mergeFailed.getMessage shouldBe "No old value specified"
      }
    }
  }

  /**
    * UpdateFunction(Some) -> Put(None, Some)
    */

  "UpdateFunction(Some) -> Put(None, Some)" when {
    "Put(None, HasTimeLeft-Greater)" in {
      val deadline = 30.seconds.fromNow
      val deadline2 = 20.seconds.fromNow
      (Memory.UpdateFunction(1, incrementBy1FunctionId, deadline), Memory.Put(1, None, deadline2)).mergeFailed.getMessage shouldBe "No old value specified"
    }

    "Put(None, HasTimeLeft-Lesser)" in {
      val deadline = 10.seconds.fromNow
      val deadline2 = 20.seconds.fromNow
      (Memory.UpdateFunction(1, incrementBy1FunctionId, deadline), Memory.Put(1, None, deadline2)).mergeFailed.getMessage shouldBe "No old value specified"
    }

    "Put(None, HasNoTimeLeft-Greater)" in {
      val deadline = 30.seconds.fromNow
      val deadline2 = 2.seconds.fromNow
      (Memory.UpdateFunction(1, incrementBy1FunctionId, deadline), Memory.Put(1, None, deadline2)).mergeFailed.getMessage shouldBe "No old value specified"
    }

    "Put(None, HasNoTimeLeft-Lesser)" in {
      val deadline = 1.seconds.fromNow
      val deadline2 = 2.seconds.fromNow
      (Memory.UpdateFunction(1, incrementBy1FunctionId, deadline), Memory.Put(1, None, deadline2)).mergeFailed.getMessage shouldBe "No old value specified"
    }

    "Put(None, Expired-Greater)" in {
      val deadline = 30.seconds.fromNow
      val deadline2 = expiredDeadline()
      (Memory.UpdateFunction(1, incrementBy1FunctionId, deadline), Memory.Put(1, None, deadline2)).mergeFailed.getMessage shouldBe "No old value specified"
    }

    "Put(None, Expired-Lesser)" in {
      val deadline2 = expiredDeadline()
      val deadline = deadline2 - 10.seconds
      (Memory.UpdateFunction(1, incrementBy1FunctionId, deadline), Memory.Put(1, None, deadline2)).mergeFailed.getMessage shouldBe "No old value specified"
    }
  }

  /**
    * UpdateFunction(Some) -> Put(Some, None)
    */

  "UpdateFunction(Some) -> Put(Some, None)" when {
    "Put(None, None)" in {
      (1 to 20) foreach {
        i =>
          //deadline for newKeyValues are not validated. HasTimeLeft, HasNoTimeLeft or Expired does have any logic for newKeyValues during merge.
          //the loop checks for all deadline conditions.
          val deadline = i.seconds.fromNow - 10.seconds //10.seconds to also account for expired deadlines.
          (Memory.UpdateFunction(i, incrementBy1FunctionId, deadline), Memory.Put(1, i, None)).merge shouldBe Memory.Put(1, i + 1, deadline)
      }
    }
  }

  /**
    * UpdateFunction(Some) -> Put(Some, Some)
    */

  "UpdateFunction(Some) -> Put(Some, Some)" when {
    "Put(Some, HasTimeLeft-Greater)" in {
      val deadline = 30.seconds.fromNow
      val deadline2 = 20.seconds.fromNow
      (Memory.UpdateFunction(1, incrementBy1FunctionId, deadline), Memory.Put(1, 1, deadline2)).merge shouldBe Memory.Put(1, 2, deadline)
    }

    "Put(Some, HasTimeLeft-Lesser)" in {
      val deadline = 10.seconds.fromNow
      val deadline2 = 20.seconds.fromNow
      (Memory.UpdateFunction(1, incrementBy1FunctionId, deadline), Memory.Put(1, 1, deadline2)).merge shouldBe Memory.Put(1, 2, deadline)
    }

    "Put(Some, HasNoTimeLeft-Greater)" in {
      val deadline = 30.seconds.fromNow
      val deadline2 = 2.seconds.fromNow
      (Memory.UpdateFunction(1, incrementBy1FunctionId, deadline), Memory.Put(1, 1, deadline2)).merge shouldBe Memory.Put(1, 2, deadline2)
    }

    "Put(Some, HasNoTimeLeft-Lesser)" in {
      val deadline = 1.seconds.fromNow
      val deadline2 = 2.seconds.fromNow
      (Memory.UpdateFunction(1, incrementBy1FunctionId, deadline), Memory.Put(1, 1, deadline2)).merge shouldBe Memory.Put(1, 2, deadline)
    }

    "Put(Some, Expired-Greater)" in {
      val deadline = 30.seconds.fromNow
      val deadline2 = expiredDeadline()
      (Memory.UpdateFunction(1, incrementBy1FunctionId, deadline), Memory.Put(1, 1, deadline2)).merge shouldBe Memory.Put(1, 2, deadline2)
    }

    "Put(Some, Expired-Lesser)" in {
      val deadline2 = expiredDeadline()
      val deadline = deadline2 - 10.seconds
      (Memory.UpdateFunction(1, incrementBy1FunctionId, deadline), Memory.Put(1, 1, deadline2)).merge shouldBe Memory.Put(1, 2, deadline)
    }
  }

}
