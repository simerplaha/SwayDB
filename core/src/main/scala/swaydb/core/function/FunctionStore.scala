/*
 * Copyright (c) 2020 Simer Plaha (@simerplaha)
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.function

import java.util.concurrent.ConcurrentHashMap

import swaydb.OK
import swaydb.core.data.{SwayFunction, Value}
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice

import scala.annotation.tailrec

trait FunctionStore {
  def get(functionId: Slice[Byte]): Option[SwayFunction]
  def put(functionId: Slice[Byte], function: SwayFunction): OK
  def exists(functionId: Slice[Byte]): Boolean
  def notExists(functionId: Slice[Byte]): Boolean =
    !exists(functionId)
}

object FunctionStore {

  def memory() =
    new MemoryStore()

  val order: FunctionIdOrder =
    new FunctionIdOrder {
      override def compare(x: Slice[Byte], y: Slice[Byte]): Int =
        KeyOrder.lexicographic.compare(x, y)
    }

  def containsFunction(functionId: Slice[Byte], values: Slice[Value]) = {

    @tailrec
    def checkContains(values: Slice[Value]): Boolean =
      values.headOption match {
        case Some(value) =>
          value match {
            case _: Value.Remove | _: Value.Update | _: Value.Put =>
              false

            case Value.Function(function, _) =>
              if (order.equiv(function, functionId))
                true
              else
                checkContains(values.dropHead())

            case Value.PendingApply(applies) =>
              checkContains(applies)
          }

        case None =>
          false
      }

    checkContains(values)
  }

  trait FunctionIdOrder extends Ordering[Slice[Byte]]
}

class MemoryStore extends FunctionStore {

  private val functions = new ConcurrentHashMap[Slice[Byte], SwayFunction]()

  override def get(functionId: Slice[Byte]): Option[SwayFunction] =
    Option(functions.get(functionId))

  override def put(functionId: Slice[Byte], function: SwayFunction): OK = {
    if (functions.putIfAbsent(functionId, function) == null)
      OK.instance
    else
      throw new Exception("Another with the same functionId exists.")
  }

  override def exists(functionId: Slice[Byte]): Boolean =
    get(functionId).isDefined
}
