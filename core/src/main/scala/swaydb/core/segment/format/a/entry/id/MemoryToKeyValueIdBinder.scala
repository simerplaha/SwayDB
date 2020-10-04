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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.core.segment.format.a.entry.id

import swaydb.core.data.Memory
import swaydb.macros.Sealed

import scala.annotation.implicitNotFound

@implicitNotFound("Type class implementation not found for MemoryToKeyValueIdBinder of type ${T}")
private[core] sealed trait MemoryToKeyValueIdBinder[+T] {
  def keyValueId: KeyValueId
}

/**
 * Glue objects to map [[Memory]] key-values to [[KeyValueId]].
 */
private[core] object MemoryToKeyValueIdBinder {

  implicit object RemoveBinder extends MemoryToKeyValueIdBinder[Memory.Remove] {
    override val keyValueId: KeyValueId = KeyValueId.Remove
  }

  implicit object UpdateBinder extends MemoryToKeyValueIdBinder[Memory.Update] {
    override val keyValueId: KeyValueId = KeyValueId.Update
  }

  implicit object FunctionBinder extends MemoryToKeyValueIdBinder[Memory.Function] {
    override val keyValueId: KeyValueId = KeyValueId.Function
  }

  implicit object RangeBinder extends MemoryToKeyValueIdBinder[Memory.Range] {
    override val keyValueId: KeyValueId = KeyValueId.Range
  }

  implicit object PutBinder extends MemoryToKeyValueIdBinder[Memory.Put] {
    override val keyValueId: KeyValueId = KeyValueId.Put
  }

  implicit object PendingApplyBinder extends MemoryToKeyValueIdBinder[Memory.PendingApply] {
    override val keyValueId: KeyValueId = KeyValueId.PendingApply
  }

  def getBinder(keyValue: Memory): MemoryToKeyValueIdBinder[Memory] =
    keyValue match {
      case _: Memory.Put =>
        implicitly[MemoryToKeyValueIdBinder[Memory.Put]]

      case _: Memory.Update =>
        implicitly[MemoryToKeyValueIdBinder[Memory.Update]]

      case _: Memory.Function =>
        implicitly[MemoryToKeyValueIdBinder[Memory.Function]]

      case _: Memory.PendingApply =>
        implicitly[MemoryToKeyValueIdBinder[Memory.PendingApply]]

      case _: Memory.Remove =>
        implicitly[MemoryToKeyValueIdBinder[Memory.Remove]]

      case _: Memory.Range =>
        implicitly[MemoryToKeyValueIdBinder[Memory.Range]]
    }

  def allBinders: List[MemoryToKeyValueIdBinder[_]] =
    Sealed.list[MemoryToKeyValueIdBinder[_]]
}
