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

import swaydb.core.TestBase
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._

class FunctionStoreSpec extends TestBase {

  "FunctionInvoke" should {
    "apply function and return new updated value" in {

      val function: Slice[Byte] => Slice[Byte] =
        (input: Slice[Byte]) => input.readInt() + 1

      FunctionStore.put("123", function).assertGet

      val value = FunctionValueSerializer.write("123")
      FunctionStore.apply(Some(Slice.writeInt(1)), value).assertGet.readInt() shouldBe 2

    }

    "apply multiple functions and return new updated value" in {
      val function1: Slice[Byte] => Slice[Byte] =
        (input: Slice[Byte]) => input.readInt() + 1

      val function2: Slice[Byte] => Slice[Byte] =
        (input: Slice[Byte]) => input.readInt() + 2

      val function3: Slice[Byte] => Slice[Byte] =
        (input: Slice[Byte]) => input.readInt() + 3


      FunctionStore.put("1", function1).assertGet
      FunctionStore.put("2", function2).assertGet
      FunctionStore.put("3", function3).assertGet

      val oneFunction = FunctionValueSerializer.write("1")
      val twoFunction = FunctionValueSerializer.write("2")
      val threeFunction = FunctionValueSerializer.write("3")
      val compose1 = FunctionValueSerializer.compose(oneFunction, twoFunction)
      val compose2 = FunctionValueSerializer.compose(compose1, threeFunction)

      FunctionStore.apply(Some(1), compose2).assertGet.readInt() shouldBe 7
    }
  }
}