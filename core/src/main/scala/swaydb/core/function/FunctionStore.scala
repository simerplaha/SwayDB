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

import java.util.concurrent.ConcurrentHashMap

import swaydb.core.util.TryUtil._
import swaydb.data.slice.Slice

import scala.util.{Failure, Try}

private[core] object FunctionStore {

  private val cache = new ConcurrentHashMap[String, Slice[Byte] => Slice[Byte]]()

  def put(id: String, function: Slice[Byte] => Slice[Byte]): Try[String] =
    if (id.contains(ComposeFunction.functionSeparator))
      Failure(new Exception(s"""FunctionId: "$id" cannot contain reserved character '${ComposeFunction.functionSeparator}'"""))
    else
      Try(cache.putIfAbsent(id, function)) map {
        _ =>
          id
      }

  def get(id: String): Option[Slice[Byte] => Slice[Byte]] =
    Option(cache.get(id))

  def containsKey(id: String): Boolean =
    cache.containsKey(id)

  def apply(oldValue: Option[Slice[Byte]],
            functionId: Slice[Byte]): Try[Option[Slice[Byte]]] =
    oldValue map {
      oldValue =>
        val classes = functionId.readString().split(ComposeFunction.functionSeparatorRegex)
        classes.toList.tryFoldLeft(oldValue) {
          case (previousValue, nextFunction) =>
            applyFunction(functionId = nextFunction, oldValue = previousValue)
        } map {
          output =>
            Some(output)
        }
    } getOrElse {
      Failure(new Exception("No old value specified"))
    }

  private def applyFunction(functionId: String,
                            oldValue: Slice[Byte]): Try[Slice[Byte]] =
    get(functionId) map {
      function =>
        Try(function(oldValue))
    } getOrElse {
      Failure(new Exception(s"Function with functionId: '$functionId' does not exist."))
    }
}