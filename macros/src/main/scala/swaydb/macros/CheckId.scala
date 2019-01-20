/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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

import scala.language.experimental.macros
import scala.reflect.macros.blackbox

class CheckId(val functionId: String)

object CheckId {

  def apply(implicit className: String): CheckId =
    new CheckId(className)

  implicit def assertFunctionId: CheckId = macro assertFunctionIdImpl

  def assertFunctionIdImpl(c: blackbox.Context): c.Expr[CheckId] = {
    import c.universe._

    val source = c.enclosingPosition.source
    val line = c.enclosingPosition.line
    val functionId = SourceReader.getFunctionId(source, line)

    if (functionId.exists(_.contains("|")))
      c.abort(
        c.enclosingPosition,
        s"""functionIds cannot contain reserved character '|'. functionId = "${functionId.getOrElse("")}""""
      )
    else
      c.Expr[CheckId](q"""${c.prefix}(${functionId.getOrElse("")})""")
  }
}