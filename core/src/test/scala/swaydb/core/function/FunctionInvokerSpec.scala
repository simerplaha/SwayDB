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

import swaydb.compiler.FunctionCompiler
import swaydb.core.{TestBase, ValueSerializerHolder_OH_SHIT}
import swaydb.serializers.Default._
import swaydb.serializers._

class FunctionInvokerSpec extends TestBase {

  "FunctionInvoke" should {
    "apply function and return new updated value" in {
      ValueSerializerHolder_OH_SHIT.valueSerializer = IntSerializer
      ValueSerializerHolder_OH_SHIT.valueType = "Int"

      val compiledFunction = FunctionCompiler.compileFunction("(int: Int) => int + 1", None, "Int").assertGet

      FunctionInvoker(Some(1), compiledFunction.className).assertGet.readInt() shouldBe 2
    }

    "apply multiple functions and return new updated value" in {
      ValueSerializerHolder_OH_SHIT.valueSerializer = IntSerializer
      ValueSerializerHolder_OH_SHIT.valueType = "Int"

      val compiledFunction1 = FunctionCompiler.compileFunction("(int: Int) => int + 1", None, "Int").assertGet.className
      val compiledFunction2 = FunctionCompiler.compileFunction("(int: Int) => int + 2", None, "Int").assertGet.className
      val compiledFunction3 = FunctionCompiler.compileFunction("(int: Int) => int + 3", None, "Int").assertGet.className

      val composedFunction = s"$compiledFunction1|$compiledFunction2|$compiledFunction3"

      FunctionInvoker(Some(1), composedFunction).assertGet.readInt() shouldBe 7
    }
  }
}