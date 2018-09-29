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

import scala.meta._
import scala.reflect.internal.util.SourceFile

object SourceReader {

  def readUpdateFunction(file: SourceFile,
                         lineNumber: Int): String = {
    val code = file.content.mkString
    var functionToApply = Option.empty[String]

    code.parse[Source].get traverse {
      case tree if tree.pos.startLine + 1 == lineNumber =>
        tree match {
          case q"$db.update($key, $name = $function)" =>
            functionToApply = Some((function: Term).toString())

          case q"$db.update($key, $function)" =>
            functionToApply = Some((function: Term).toString())

          case q"$db.update($from, $to, $name = $function)" =>
            functionToApply = Some((function: Term).toString())

          case q"$db.update($from, $to, $function)" =>
            functionToApply = Some((function: Term).toString())

          case _ =>
            ()
        }
    }
    functionToApply getOrElse {
      throw new Exception(s"Failed to parse function at lineNumber: $lineNumber")
    }
  }
}
