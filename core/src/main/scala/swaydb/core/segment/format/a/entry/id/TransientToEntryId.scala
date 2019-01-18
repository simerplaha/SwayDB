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
