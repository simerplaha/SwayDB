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

class KeyValueMerger6_UpdateFunction_None_Into_Remove_Spec extends WordSpec with Matchers with CommonAssertions {

  /**
    * UpdateFunction(None) -> Remove(None)
    */

  "UpdateFunction(None) -> Remove(None)" when {
    "Remove(None)" in {
      (Memory.UpdateFunction(1, incrementBy1FunctionId, None), Memory.Remove(1, None)).merge shouldBe Memory.Remove(1, None)
    }
  }

  /**
    * UpdateFunction(None) -> Remove(Some)
    */

  "UpdateFunction(None) -> Remove(Some)" when {
    "Remove(HasTimeLeft)" in {
      val deadline = 20.seconds.fromNow
      (Memory.UpdateFunction(1, incrementBy1FunctionId, None), Memory.Remove(1, deadline)).merge shouldBe Memory.UpdateFunction(1, incrementBy1FunctionId, deadline)
    }

    "Remove(HasNoTimeLeft)" in {
      val deadline = 5.seconds.fromNow
      (Memory.UpdateFunction(1, incrementBy1FunctionId, None), Memory.Remove(1, deadline)).merge shouldBe Memory.UpdateFunction(1, incrementBy1FunctionId, deadline)
    }

    "Remove(Expired)" in {
      val deadline = expiredDeadline()
      (Memory.UpdateFunction(1, incrementBy1FunctionId, None), Memory.Remove(1, deadline)).merge shouldBe Memory.Remove(1, deadline)
    }
  }
}
