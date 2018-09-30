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

package swaydb.core.segment.format.one.entry.id

import EntryId.{Deadline, Key, Value}
import swaydb.macros.SealedList
import swaydb.core.util.PipeOps._

sealed abstract class RemoveEntryId(override val id: Int) extends EntryId(id)
object RemoveEntryId {

  sealed trait KeyPartiallyCompressed extends Key.PartiallyCompressed {
    //@formatter:off
    //TO-DO instead of throwing exception this should be type safe
    override def valueFullyCompressed: Value.FullyCompressed = throw new Exception("Ugly! But this should never be called. Remove key-values do not have values.")
    override def valueUncompressed: Value.Uncompressed = throw new Exception("Ugly! But this should never be called. Remove key-values do not have values.")
    override val noValue: Value.NoValue = KeyPartiallyCompressed.NoValue
    //@formatter:on
  }
  object KeyPartiallyCompressed extends KeyPartiallyCompressed {
    sealed trait NoValue extends Value.NoValue with KeyPartiallyCompressed {
      override val noDeadline: Deadline.NoDeadline = NoValue.NoDeadline
      override val deadlineOneCompressed: Deadline.OneCompressed = NoValue.DeadlineOneCompressed
      override val deadlineTwoCompressed: Deadline.TwoCompressed = NoValue.DeadlineTwoCompressed
      override val deadlineThreeCompressed: Deadline.ThreeCompressed = NoValue.DeadlineThreeCompressed
      override val deadlineFourCompressed: Deadline.FourCompressed = NoValue.DeadlineFourCompressed
      override val deadlineFiveCompressed: Deadline.FiveCompressed = NoValue.DeadlineFiveCompressed
      override val deadlineSixCompressed: Deadline.SixCompressed = NoValue.DeadlineSixCompressed
      override val deadlineSevenCompressed: Deadline.SevenCompressed = NoValue.DeadlineSevenCompressed
      override val deadlineFullyCompressed: Deadline.FullyCompressed = NoValue.DeadlineFullyCompressed
      override val deadlineUncompressed: Deadline.Uncompressed = NoValue.DeadlineUncompressed
    }
    object NoValue extends NoValue {
      case object NoDeadline extends RemoveEntryId(0) with Deadline.NoDeadline with NoValue
      case object DeadlineOneCompressed extends RemoveEntryId(1) with Deadline.OneCompressed with NoValue
      case object DeadlineTwoCompressed extends RemoveEntryId(2) with Deadline.TwoCompressed with NoValue
      case object DeadlineThreeCompressed extends RemoveEntryId(3) with Deadline.ThreeCompressed with NoValue
      case object DeadlineFourCompressed extends RemoveEntryId(4) with Deadline.FourCompressed with NoValue
      case object DeadlineFiveCompressed extends RemoveEntryId(5) with Deadline.FiveCompressed with NoValue
      case object DeadlineSixCompressed extends RemoveEntryId(6) with Deadline.SixCompressed with NoValue
      case object DeadlineSevenCompressed extends RemoveEntryId(7) with Deadline.SevenCompressed with NoValue
      case object DeadlineFullyCompressed extends RemoveEntryId(8) with Deadline.FullyCompressed with NoValue
      case object DeadlineUncompressed extends RemoveEntryId(9) with Deadline.Uncompressed with NoValue
    }
  }

  sealed trait KeyUncompressed extends Key.Uncompressed {
    //@formatter:off
    //TO-DO instead of throwing exception this should be type safe
    override def valueFullyCompressed: Value.FullyCompressed = throw new Exception("Ugly! But this should never be called. Remove key-values do not have values.")
    override def valueUncompressed: Value.Uncompressed = throw new Exception("Ugly! But this should never be called. Remove key-values do not have values.")
    override val noValue: Value.NoValue = KeyUncompressed.NoValue
    //@formatter:on
  }
  object KeyUncompressed extends KeyUncompressed {
    sealed trait NoValue extends Value.NoValue with KeyUncompressed {
      override val noDeadline: Deadline.NoDeadline = NoValue.NoDeadline
      override val deadlineOneCompressed: Deadline.OneCompressed = NoValue.DeadlineOneCompressed
      override val deadlineTwoCompressed: Deadline.TwoCompressed = NoValue.DeadlineTwoCompressed
      override val deadlineThreeCompressed: Deadline.ThreeCompressed = NoValue.DeadlineThreeCompressed
      override val deadlineFourCompressed: Deadline.FourCompressed = NoValue.DeadlineFourCompressed
      override val deadlineFiveCompressed: Deadline.FiveCompressed = NoValue.DeadlineFiveCompressed
      override val deadlineSixCompressed: Deadline.SixCompressed = NoValue.DeadlineSixCompressed
      override val deadlineSevenCompressed: Deadline.SevenCompressed = NoValue.DeadlineSevenCompressed
      override val deadlineFullyCompressed: Deadline.FullyCompressed = NoValue.DeadlineFullyCompressed
      override val deadlineUncompressed: Deadline.Uncompressed = NoValue.DeadlineUncompressed
    }
    object NoValue extends NoValue {
      case object NoDeadline extends RemoveEntryId(10) with Deadline.NoDeadline with NoValue
      case object DeadlineOneCompressed extends RemoveEntryId(11) with Deadline.OneCompressed with NoValue
      case object DeadlineTwoCompressed extends RemoveEntryId(12) with Deadline.TwoCompressed with NoValue
      case object DeadlineThreeCompressed extends RemoveEntryId(13) with Deadline.ThreeCompressed with NoValue
      case object DeadlineFourCompressed extends RemoveEntryId(14) with Deadline.FourCompressed with NoValue
      case object DeadlineFiveCompressed extends RemoveEntryId(15) with Deadline.FiveCompressed with NoValue
      case object DeadlineSixCompressed extends RemoveEntryId(16) with Deadline.SixCompressed with NoValue
      case object DeadlineSevenCompressed extends RemoveEntryId(17) with Deadline.SevenCompressed with NoValue
      case object DeadlineFullyCompressed extends RemoveEntryId(18) with Deadline.FullyCompressed with NoValue
      case object DeadlineUncompressed extends RemoveEntryId(19) with Deadline.Uncompressed with NoValue
    }
  }

  sealed trait KeyFullyCompressed extends Key.FullyCompressed {
    //@formatter:off
    //TO-DO instead of throwing exception this should be type safe
    override def valueFullyCompressed: Value.FullyCompressed = throw new Exception("Ugly! But this should never be called. Remove key-values do not have values.")
    override def valueUncompressed: Value.Uncompressed = throw new Exception("Ugly! But this should never be called. Remove key-values do not have values.")
    override val noValue: Value.NoValue = KeyFullyCompressed.NoValue
    //@formatter:on
  }
  object KeyFullyCompressed extends KeyFullyCompressed {
    sealed trait NoValue extends Value.NoValue with KeyFullyCompressed {
      override val noDeadline: Deadline.NoDeadline = NoValue.NoDeadline
      override val deadlineOneCompressed: Deadline.OneCompressed = NoValue.DeadlineOneCompressed
      override val deadlineTwoCompressed: Deadline.TwoCompressed = NoValue.DeadlineTwoCompressed
      override val deadlineThreeCompressed: Deadline.ThreeCompressed = NoValue.DeadlineThreeCompressed
      override val deadlineFourCompressed: Deadline.FourCompressed = NoValue.DeadlineFourCompressed
      override val deadlineFiveCompressed: Deadline.FiveCompressed = NoValue.DeadlineFiveCompressed
      override val deadlineSixCompressed: Deadline.SixCompressed = NoValue.DeadlineSixCompressed
      override val deadlineSevenCompressed: Deadline.SevenCompressed = NoValue.DeadlineSevenCompressed
      override val deadlineFullyCompressed: Deadline.FullyCompressed = NoValue.DeadlineFullyCompressed
      override val deadlineUncompressed: Deadline.Uncompressed = NoValue.DeadlineUncompressed
    }
    object NoValue extends NoValue {
      case object NoDeadline extends RemoveEntryId(20) with Deadline.NoDeadline with NoValue
      case object DeadlineOneCompressed extends RemoveEntryId(21) with Deadline.OneCompressed with NoValue
      case object DeadlineTwoCompressed extends RemoveEntryId(22) with Deadline.TwoCompressed with NoValue
      case object DeadlineThreeCompressed extends RemoveEntryId(23) with Deadline.ThreeCompressed with NoValue
      case object DeadlineFourCompressed extends RemoveEntryId(24) with Deadline.FourCompressed with NoValue
      case object DeadlineFiveCompressed extends RemoveEntryId(25) with Deadline.FiveCompressed with NoValue
      case object DeadlineSixCompressed extends RemoveEntryId(26) with Deadline.SixCompressed with NoValue
      case object DeadlineSevenCompressed extends RemoveEntryId(27) with Deadline.SevenCompressed with NoValue
      case object DeadlineFullyCompressed extends RemoveEntryId(28) with Deadline.FullyCompressed with NoValue
      case object DeadlineUncompressed extends RemoveEntryId(29) with Deadline.Uncompressed with NoValue
    }
  }

  def keyIdsList: List[RemoveEntryId] = SealedList.list[RemoveEntryId].sortBy(_.id)

  private val (headId, lastId) = keyIdsList ==> {
    keyIdsList =>
      (keyIdsList.head.id, keyIdsList.last.id)
  }

  def contains(id: Int): Option[Int] =
    if (id >= headId && id <= lastId)
      Some(id)
    else
      None
}