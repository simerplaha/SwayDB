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

package swaydb.macros

import java.nio.file.Paths

import swaydb.compiler.FunctionCompiler

import scala.language.experimental.macros
import scala.reflect.macros.blackbox
import scala.util.{Failure, Success}

class UpdateFunction[V](val className: String)

object UpdateFunction {

  val testDir =
    Paths.get(getClass.getClassLoader.getResource("").getPath)
      .getParent
      .resolve("DYNAMIC_CLASSES")

  val compiler = new FunctionCompiler(testDir)

  def apply[V](implicit className: String): UpdateFunction[V] =
    new UpdateFunction(className)

  implicit def compile[V]: UpdateFunction[V] = macro doCompile[V]

  def doCompile[V: c.WeakTypeTag](c: blackbox.Context): c.Expr[UpdateFunction[V]] = {
    import c.universe._

    val symbol = weakTypeOf[V].typeSymbol
    val source = c.enclosingPosition.source
    val line = c.enclosingPosition.line
    val sourceString = SourceReader.readUpdateFunction(source, line)
    val valueTypeName = symbol.fullName.toString

    compiler.compileFunction(sourceString, Some(Seq(valueTypeName)), valueTypeName) match {
      case Success(compiledFunction) =>
        c.Expr[UpdateFunction[V]](q"""${c.prefix}(${compiledFunction.className})""")
      case Failure(exception) =>
        throw exception
    }
  }
}