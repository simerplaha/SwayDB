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

import swaydb.core.segment.data.Persistent
import swaydb.macros.MacroSealed

import scala.annotation.implicitNotFound

@implicitNotFound("Type class implementation not found for PersistentToKeyValueIdBinder of type ${T}")
private[core] sealed trait PersistentToKeyValueIdBinder[T] {
  val keyValueId: KeyValueId
}

/**
 * Glue objects to map [[Persistent]] key-values to [[KeyValueId]].
 */
private[core] object PersistentToKeyValueIdBinder {

  implicit object RemoveBinder extends PersistentToKeyValueIdBinder[Persistent.Remove] {
    override val keyValueId: KeyValueId = KeyValueId.Remove
  }

  implicit object UpdateBinder extends PersistentToKeyValueIdBinder[Persistent.Update] {
    override val keyValueId: KeyValueId = KeyValueId.Update
  }

  implicit object FunctionBinder extends PersistentToKeyValueIdBinder[Persistent.Function] {
    override val keyValueId: KeyValueId = KeyValueId.Function
  }

  implicit object RangeBinder extends PersistentToKeyValueIdBinder[Persistent.Range] {
    override val keyValueId: KeyValueId = KeyValueId.Range
  }

  implicit object PutBinder extends PersistentToKeyValueIdBinder[Persistent.Put] {
    override val keyValueId: KeyValueId = KeyValueId.Put
  }

  implicit object PendingApplyBinder extends PersistentToKeyValueIdBinder[Persistent.PendingApply] {
    override val keyValueId: KeyValueId = KeyValueId.PendingApply
  }

  def allBinders: List[PersistentToKeyValueIdBinder[_]] =
    MacroSealed.list[PersistentToKeyValueIdBinder[_]]
}
