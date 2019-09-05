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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.segment.format.a.entry.id

import swaydb.core.data.Persistent
import swaydb.macros.SealedList

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

  implicit object GroupBinder extends PersistentToKeyValueIdBinder[Persistent.Group] {
    override val keyValueId: KeyValueId = KeyValueId.Group
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
    SealedList.list[PersistentToKeyValueIdBinder[_]]
}
