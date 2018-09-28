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

package swaydb.compiler

import java.nio.file.{Files, Paths}

import org.scalatest.{Matchers, WordSpec}

class ScalaCompilerSpec extends WordSpec with Matchers with TryAssert {

  val testDir =
    Paths.get(getClass.getClassLoader.getResource("").getPath)
      .getParent
      .resolve("DYNAMIC_CLASSES")

  val compiler = new ScalaCompiler(testDir)

  "compile function" in {
    val source = "(variable: Int) => variable.toString"
    val compiledFunction = compiler.compileFunction(source, "String").assertGet
    compiler.getFunction1[Int, String](compiledFunction.className).assertGet(123) shouldBe "123"
  }

  "compile function in a block" in {
    val source = "{ (variable: String) => {variable.toInt} }"
    val compiledFunction = compiler.compileFunction(source, "Int").assertGet
    compiler.getFunction1[String, Int](compiledFunction.className).assertGet("123") shouldBe 123
  }

  "remove function" in {
    val source = "(variable: String, anotherInput: Int) => {variable.toInt}"
    val compiledFunction = compiler.compileFunction(source, "Int").assertGet
    compiler.getFunction2[String, Int, Int](compiledFunction.className).assertGet("123", 1) shouldBe 123

    Files.exists(testDir.resolve(compiledFunction.className + ".class")) shouldBe true

    compiler.removeClass(compiledFunction.className)
    compiler.getFunction2[String, Int, Int](compiledFunction.className).assertGetOpt shouldBe empty

    Files.exists(testDir.resolve(compiledFunction.className + ".class")) shouldBe false
  }



}
