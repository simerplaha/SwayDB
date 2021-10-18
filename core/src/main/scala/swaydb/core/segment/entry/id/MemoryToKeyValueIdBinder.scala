/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb.core.segment.entry.id

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
