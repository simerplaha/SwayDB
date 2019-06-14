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

import scala.annotation.implicitNotFound
import swaydb.core.data.Transient
import swaydb.macros.SealedList

@implicitNotFound("Type class implementation not found for TransientToKeyValueIdBinder of type ${T}")
private[core] sealed trait TransientToKeyValueIdBinder[T] {
  val keyValueId: KeyValueId
}

/**
  * Glue objects to map [[Transient]] key-values to [[KeyValueId]].
  */
private[core] object TransientToKeyValueIdBinder {

  implicit object Remove extends TransientToKeyValueIdBinder[Transient.Remove] {
    override val keyValueId: KeyValueId = KeyValueId.Remove
  }

  implicit object Update extends TransientToKeyValueIdBinder[Transient.Update] {
    override val keyValueId: KeyValueId = KeyValueId.Update
  }

  implicit object Function extends TransientToKeyValueIdBinder[Transient.Function] {
    override val keyValueId: KeyValueId = KeyValueId.Function
  }

  implicit object Group extends TransientToKeyValueIdBinder[Transient.Group] {
    override val keyValueId: KeyValueId = KeyValueId.Group
  }

  implicit object Range extends TransientToKeyValueIdBinder[Transient.Range] {
    override val keyValueId: KeyValueId = KeyValueId.Range
  }

  implicit object Put extends TransientToKeyValueIdBinder[Transient.Put] {
    override val keyValueId: KeyValueId = KeyValueId.Put
  }

  implicit object PendingApply extends TransientToKeyValueIdBinder[Transient.PendingApply] {
    override val keyValueId: KeyValueId = KeyValueId.PendingApply
  }

  def all: List[TransientToKeyValueIdBinder[_]] =
    SealedList.list[TransientToKeyValueIdBinder[_]]
}
