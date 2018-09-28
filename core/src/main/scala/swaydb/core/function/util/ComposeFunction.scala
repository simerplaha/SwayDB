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

package swaydb.core.function.util

import swaydb.core.data.KeyValue

import scala.util.{Failure, Success, Try}

object ComposeFunction {

  def getFunctionClassName(function: KeyValue.ReadOnly.UpdateFunction): Try[String] =
    function.getOrFetchValue flatMap {
      leftOption =>
        leftOption map {
          left =>
            Success(left.readString())
        } getOrElse {
          Failure(new Exception("No function"))
        }
    }

  def apply(left: KeyValue.ReadOnly.UpdateFunction,
            right: KeyValue.ReadOnly.UpdateFunction) =
    for {
      leftFunction <- getFunctionClassName(left)
      rightFunction <- getFunctionClassName(right)
    } yield {
      s"$leftFunction|$rightFunction"
    }
}
