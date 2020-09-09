/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.core.function

import java.util.concurrent.ConcurrentHashMap

import swaydb.OK
import swaydb.core.data.{SwayFunction, Value}
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice

import scala.annotation.tailrec
import scala.collection.mutable
import scala.jdk.CollectionConverters.ConcurrentMapHasAsScala

/**
 * Stores all functions currently registered. This should always contain
 * all functions currently applied or being applied during compaction.
 *
 * Missing functions will be reported with their functionId.
 */
private[swaydb] abstract class FunctionStore {
  def get(functionId: Slice[Byte]): Option[SwayFunction]
  def put(functionId: Slice[Byte], function: SwayFunction): OK
  def remove(functionId: Slice[Byte]): SwayFunction

  def contains(functionId: Slice[Byte]): Boolean
  def notContains(functionId: Slice[Byte]): Boolean =
    !contains(functionId)

  def asScala: mutable.Map[Slice[Byte], SwayFunction]

  def size: Int
}

private[swaydb] object FunctionStore {

  def memory(): FunctionStore.Memory =
    new Memory()

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
              order.equiv(function, functionId) || checkContains(values.dropHead())

            case Value.PendingApply(applies) =>
              checkContains(applies)
          }

        case None =>
          false
      }

    checkContains(values)
  }

  trait FunctionIdOrder extends Ordering[Slice[Byte]]

  final class Memory extends FunctionStore {

    val hashMap = new ConcurrentHashMap[Slice[Byte], SwayFunction]()

    override def get(functionId: Slice[Byte]): Option[SwayFunction] =
      Option(hashMap.get(functionId))

    override def put(functionId: Slice[Byte], function: SwayFunction): OK =
      if (hashMap.putIfAbsent(functionId, function) == null)
        OK.instance
      else
        throw new Exception("Another with the same functionId exists.")

    override def contains(functionId: Slice[Byte]): Boolean =
      get(functionId).isDefined

    override def remove(functionId: Slice[Byte]): SwayFunction =
      hashMap.remove(functionId)

    def asScala: mutable.Map[Slice[Byte], SwayFunction] =
      hashMap.asScala

    def size =
      hashMap.size()
  }
}
