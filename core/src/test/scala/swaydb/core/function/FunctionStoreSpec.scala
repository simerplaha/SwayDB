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

package swaydb.core.function

import swaydb.core.{TestBase, ValueSerializerHolder_OH_SHIT}
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._

class FunctionStoreSpec extends TestBase {

  "FunctionInvoke" should {
    "apply function and return new updated value" in {
      ValueSerializerHolder_OH_SHIT.valueSerializer = IntSerializer
      ValueSerializerHolder_OH_SHIT.valueType = "Int"

      FunctionStore.put("123", ((int: Int) => int + 1).asInstanceOf[Any => Any])

      FunctionStore.apply(Some(Slice.writeInt(1)), "123").assertGet.readInt() shouldBe 2

    }

    "apply multiple functions and return new updated value" in {
      ValueSerializerHolder_OH_SHIT.valueSerializer = IntSerializer
      ValueSerializerHolder_OH_SHIT.valueType = "Int"

      FunctionStore.put("1", ((int: Int) => int + 1).asInstanceOf[Any => Any])
      FunctionStore.put("2", ((int: Int) => int + 2).asInstanceOf[Any => Any])
      FunctionStore.put("3", ((int: Int) => int + 3).asInstanceOf[Any => Any])

      val composedFunction = Seq("1", "2", "3").mkString(ComposeFunction.functionSeparator)

      FunctionStore.apply(Some(1), composedFunction).assertGet.readInt() shouldBe 7
    }
  }
}