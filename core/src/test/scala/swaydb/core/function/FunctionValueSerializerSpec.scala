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

class FunctionValueSerializerSpec extends TestBase {

  "FunctionValueSerializer" when {
    "writing single function" should {

      def doAssert(functionId: String) = {
        val value = FunctionValueSerializer.write(functionId)

        val functions = FunctionValueSerializer.read(value).assertGet
        functions should have size 1

        val (insertId, readFunctionId) = functions.head
        insertId should not be empty
        readFunctionId shouldBe functionId
      }

      "write and read a single functionId with insertId" in {
        doAssert(functionId = randomCharacters())
      }

      "write and read a empty functionId with insertId" in {
        doAssert(functionId = "")
      }

      "write and read a large single functionId with insertId" in {
        doAssert(functionId = randomCharacters(10000))
      }
    }

    "composing function" should {

      "write and read a multiple functions" in {
        val functionId1 = randomCharacters()
        val functionId2 = randomCharacters()
        val functionId3 = randomCharacters()
        val functionId4 = randomCharacters()
        val functionId1Bytes = FunctionValueSerializer.write(functionId1)
        val functionId2Bytes = FunctionValueSerializer.write(functionId2)
        val functionId3Bytes = FunctionValueSerializer.write(functionId3)
        val functionId4Bytes = FunctionValueSerializer.write(functionId4)

        val composeFunction1 = FunctionValueSerializer.compose(Some(functionId1Bytes), Some(functionId2Bytes)).assertGet
        val composedFunction2 = FunctionValueSerializer.compose(Some(composeFunction1), Some(functionId3Bytes)).assertGet
        val composedFunction3 = FunctionValueSerializer.compose(Some(composedFunction2), Some(functionId4Bytes)).assertGet

        val functions = FunctionValueSerializer.read(composedFunction3).assertGet.toArray
        functions should have size 4

        val (firstFunctionInsertId, firstFunctionId) = functions.head
        val (secondFunctionInsertId, secondFunctionId) = functions(1)
        val (thirdFunctionInsertId, thirdFunctionId) = functions(2)
        val (fourthFunctionInsertId, fourthFunctionId) = functions(3)

        firstFunctionInsertId should not be empty
        secondFunctionInsertId should not be empty
        thirdFunctionInsertId should not be empty
        fourthFunctionInsertId should not be empty

        firstFunctionId shouldBe functionId1
        secondFunctionId shouldBe functionId2
        thirdFunctionId shouldBe functionId3
        fourthFunctionId shouldBe functionId4
      }
    }
  }
}