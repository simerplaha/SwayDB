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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.segment.format.a.entry.id

import scala.annotation.implicitNotFound
import swaydb.core.data.Transient

@implicitNotFound("Type class implementation not found for TransientToEntryId of type ${T}")
sealed trait TransientToEntryId[T] {
  val id: EntryId.Id
}

object TransientToEntryId {

  implicit object Remove extends TransientToEntryId[Transient.Remove] {
    override val id: EntryId.Id = EntryId.Remove
  }

  implicit object Update extends TransientToEntryId[Transient.Update] {
    override val id: EntryId.Id = EntryId.Update
  }

  implicit object Function extends TransientToEntryId[Transient.Function] {
    override val id: EntryId.Id = EntryId.Function
  }

  implicit object Group extends TransientToEntryId[Transient.Group] {
    override val id: EntryId.Id = EntryId.Group
  }

  implicit object Range extends TransientToEntryId[Transient.Range] {
    override val id: EntryId.Id = EntryId.Range
  }

  implicit object Put extends TransientToEntryId[Transient.Put] {
    override val id: EntryId.Id = EntryId.Put
  }

  implicit object PendingApply extends TransientToEntryId[Transient.PendingApply] {
    override val id: EntryId.Id = EntryId.PendingApply
  }
}
