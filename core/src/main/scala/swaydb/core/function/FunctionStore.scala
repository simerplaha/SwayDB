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

import swaydb.core.ValueSerializerHolder_OH_SHIT
import swaydb.core.util.TryUtil._
import swaydb.data.slice.Slice
import swaydb.serializers.Serializer

import scala.util.{Failure, Try}

private[core] object FunctionStore {

  private val cache = new ConcurrentHashMap[String, Any => Any]()

  def put(id: String, function: Any => Any): Try[String] =
    if (id.contains(ComposeFunction.functionSeparator))
      Failure(new Exception(s"""FunctionId: "$id" cannot contain reserved character '${ComposeFunction.functionSeparator}'"""))
    else
      Try(cache.putIfAbsent(id, function)) map {
        _ =>
          id
      }

  def get(id: String): Option[Any => Any] =
    Option(cache.get(id))

  def containsKey(id: String): Boolean =
    cache.containsKey(id)

  def apply(oldValue: Option[Slice[Byte]],
            functionId: Slice[Byte]): Try[Option[Slice[Byte]]] =
    oldValue map {
      oldValueBytes =>
        val classes = functionId.readString().split(ComposeFunction.functionSeparatorRegex)
        val oldValue = ValueSerializerHolder_OH_SHIT.valueSerializer.read(oldValueBytes)
        classes.toList.tryFoldLeft(oldValue) {
          case (previousValue, nextFunction) =>
            applyFunction(functionId = nextFunction, oldValue = previousValue)
        } flatMap {
          case Failure(failure) =>
            Failure(failure)
          case newValue =>
            Try(ValueSerializerHolder_OH_SHIT.valueSerializer.asInstanceOf[Serializer[Any]].write(newValue))
        } map {
          output =>
            Some(output)
        }
    } getOrElse {
      Failure(new Exception("No old value specified"))
    }

  private def applyFunction(functionId: String,
                            oldValue: Any): Try[Any] =
    get(functionId) map {
      function =>
        Try(function(oldValue))
    } getOrElse {
      Failure(new Exception(s"Function with functionId: '$functionId' does not exist."))
    }
}