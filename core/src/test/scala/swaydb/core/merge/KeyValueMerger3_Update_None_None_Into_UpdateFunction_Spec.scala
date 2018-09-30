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

class KeyValueMerger3_Update_None_None_Into_UpdateFunction_Spec extends WordSpec with Matchers with CommonAssertions {

  /**
    * Update(None, None) -> UpdateFunction(None)
    */

  "Update(None, None) -> UpdateFunction(None)" when {
    "UpdateFunction(None)" in {
      (Memory.Update(1, None, None), Memory.UpdateFunction(1, incrementBy1FunctionId, None)).merge shouldBe Memory.Update(1, None, None)
    }
  }

  /**
    * Update(None, None) -> UpdateFunction(Some)
    */

  "Update(None, None) -> UpdateFunction(Some)" when {
    "UpdateFunction(HasTimeLeft)" in {
      val deadline = 20.seconds.fromNow
      (Memory.Update(1, None, None), Memory.UpdateFunction(1, incrementBy1FunctionId, deadline)).merge shouldBe Memory.Update(1, None, deadline)
    }

    "UpdateFunction(HasNoTimeLeft)" in {
      val deadline = 5.seconds.fromNow
      (Memory.Update(1, None, None), Memory.UpdateFunction(1, incrementBy1FunctionId, deadline)).merge shouldBe Memory.Update(1, None, deadline)
    }

    "UpdateFunction(Expired)" in {
      val deadline = expiredDeadline()
      (Memory.Update(1, None, None), Memory.UpdateFunction(1, incrementBy1FunctionId, deadline)).merge shouldBe Memory.Update(1, None, deadline)
    }
  }
}
