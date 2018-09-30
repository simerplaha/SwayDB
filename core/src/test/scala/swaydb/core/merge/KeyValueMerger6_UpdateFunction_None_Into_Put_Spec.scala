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

class KeyValueMerger6_UpdateFunction_None_Into_Put_Spec extends WordSpec with Matchers with CommonAssertions {

  /**
    * UpdateFunction(None) -> Put(None, None)
    */

  "UpdateFunction(None) -> Put(None, None)" when {
    "Put(None, None)" in {
      (Memory.UpdateFunction(1, incrementBy1FunctionId, None), Memory.Put(1, None, None)).mergeFailed.getMessage shouldBe "No old value specified"
    }
  }

  /**
    * UpdateFunction(None) -> Put(None, Some)
    */

  "UpdateFunction(None) -> Put(None, Some)" when {
    "Put(None, HasTimeLeft)" in {
      val deadline = 20.seconds.fromNow
      (Memory.UpdateFunction(1, incrementBy1FunctionId, None), Memory.Put(1, None, deadline)).mergeFailed.getMessage shouldBe "No old value specified"
    }

    "Put(None, HasNoTimeLeft)" in {
      val deadline = 5.seconds.fromNow
      (Memory.UpdateFunction(1, incrementBy1FunctionId, None), Memory.Put(1, None, deadline)).mergeFailed.getMessage shouldBe "No old value specified"
    }

    "Put(None, Expired)" in {
      val deadline = expiredDeadline()
      (Memory.UpdateFunction(1, incrementBy1FunctionId, None), Memory.Put(1, None, deadline)).mergeFailed.getMessage shouldBe "No old value specified"
    }
  }

  /**
    * UpdateFunction(None) -> Put(Some, None)
    */

  "UpdateFunction(None) -> Put(Some, None)" when {
    "Put(Some, None)" in {
      (Memory.UpdateFunction(1, incrementBy1FunctionId, None), Memory.Put(1, 2, None)).merge shouldBe Memory.Put(1, 3, None)
    }
  }

  "UpdateFunction(None) -> Put(Some, Some)" when {
    "Put(None, HasTimeLeft)" in {
      val deadline = 20.seconds.fromNow
      (Memory.UpdateFunction(1, incrementBy1FunctionId, None), Memory.Put(1, 1, deadline)).merge shouldBe Memory.Put(1, 2, deadline)
    }

    "Put(None, HasNoTimeLeft)" in {
      val deadline = 5.seconds.fromNow
      (Memory.UpdateFunction(1, incrementBy1FunctionId, None), Memory.Put(1, 1, deadline)).merge shouldBe Memory.Put(1, 2, deadline)
    }

    "Put(None, Expired)" in {
      val deadline = expiredDeadline()
      (Memory.UpdateFunction(1, incrementBy1FunctionId, None), Memory.Put(1, 1, deadline)).merge shouldBe Memory.Put(1, 2, deadline)
    }
  }

}
