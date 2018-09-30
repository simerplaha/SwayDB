
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

/** ******************************************
  * ************ GENERATED CLASS *************
  * ******************************************/

import swaydb.core.segment.format.one.entry.id.EntryId._
import swaydb.macros.SealedList
import swaydb.core.util.PipeOps._

/**
  * This code is not used in actual production.
  *
  * It is a base template class for generating IDs for all other key-value type using
  * [[swaydb.core.segment.format.one.entry.generators.IdsGenerator]] which gives all key-values types unique ids.
  *
  * will remove that line for the target generated class.
  *
  * TO DO - switch to using macros.
  */
sealed abstract class GroupKeyPartiallyCompressedEntryId(override val id: Int) extends EntryId(id)
object GroupKeyPartiallyCompressedEntryId {

  sealed trait KeyPartiallyCompressed extends Key.PartiallyCompressed {
    override val valueFullyCompressed: Value.FullyCompressed = KeyPartiallyCompressed.ValueFullyCompressed
    override val valueUncompressed: Value.Uncompressed = KeyPartiallyCompressed.ValueUncompressed
    override val noValue: Value.NoValue = KeyPartiallyCompressed.NoValue
  }
  case object KeyPartiallyCompressed extends KeyPartiallyCompressed {

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
      case object NoDeadline extends GroupKeyPartiallyCompressedEntryId(693) with Deadline.NoDeadline with NoValue
      case object DeadlineOneCompressed extends GroupKeyPartiallyCompressedEntryId(694) with Deadline.OneCompressed with NoValue
      case object DeadlineTwoCompressed extends GroupKeyPartiallyCompressedEntryId(695) with Deadline.TwoCompressed with NoValue
      case object DeadlineThreeCompressed extends GroupKeyPartiallyCompressedEntryId(696) with Deadline.ThreeCompressed with NoValue
      case object DeadlineFourCompressed extends GroupKeyPartiallyCompressedEntryId(697) with Deadline.FourCompressed with NoValue
      case object DeadlineFiveCompressed extends GroupKeyPartiallyCompressedEntryId(698) with Deadline.FiveCompressed with NoValue
      case object DeadlineSixCompressed extends GroupKeyPartiallyCompressedEntryId(699) with Deadline.SixCompressed with NoValue
      case object DeadlineSevenCompressed extends GroupKeyPartiallyCompressedEntryId(700) with Deadline.SevenCompressed with NoValue
      case object DeadlineFullyCompressed extends GroupKeyPartiallyCompressedEntryId(701) with Deadline.FullyCompressed with NoValue
      case object DeadlineUncompressed extends GroupKeyPartiallyCompressedEntryId(702) with Deadline.Uncompressed with NoValue
    }

    sealed trait ValueFullyCompressed extends Value.FullyCompressed with KeyPartiallyCompressed {
      override val noDeadline: Deadline.NoDeadline = ValueFullyCompressed.NoDeadline
      override val deadlineOneCompressed: Deadline.OneCompressed = ValueFullyCompressed.DeadlineOneCompressed
      override val deadlineTwoCompressed: Deadline.TwoCompressed = ValueFullyCompressed.DeadlineTwoCompressed
      override val deadlineThreeCompressed: Deadline.ThreeCompressed = ValueFullyCompressed.DeadlineThreeCompressed
      override val deadlineFourCompressed: Deadline.FourCompressed = ValueFullyCompressed.DeadlineFourCompressed
      override val deadlineFiveCompressed: Deadline.FiveCompressed = ValueFullyCompressed.DeadlineFiveCompressed
      override val deadlineSixCompressed: Deadline.SixCompressed = ValueFullyCompressed.DeadlineSixCompressed
      override val deadlineSevenCompressed: Deadline.SevenCompressed = ValueFullyCompressed.DeadlineSevenCompressed
      override val deadlineFullyCompressed: Deadline.FullyCompressed = ValueFullyCompressed.DeadlineFullyCompressed
      override val deadlineUncompressed: Deadline.Uncompressed = ValueFullyCompressed.DeadlineUncompressed
    }
    object ValueFullyCompressed extends ValueFullyCompressed {
      case object NoDeadline extends GroupKeyPartiallyCompressedEntryId(703) with Deadline.NoDeadline with ValueFullyCompressed
      case object DeadlineOneCompressed extends GroupKeyPartiallyCompressedEntryId(704) with Deadline.OneCompressed with ValueFullyCompressed
      case object DeadlineTwoCompressed extends GroupKeyPartiallyCompressedEntryId(705) with Deadline.TwoCompressed with ValueFullyCompressed
      case object DeadlineThreeCompressed extends GroupKeyPartiallyCompressedEntryId(706) with Deadline.ThreeCompressed with ValueFullyCompressed
      case object DeadlineFourCompressed extends GroupKeyPartiallyCompressedEntryId(707) with Deadline.FourCompressed with ValueFullyCompressed
      case object DeadlineFiveCompressed extends GroupKeyPartiallyCompressedEntryId(708) with Deadline.FiveCompressed with ValueFullyCompressed
      case object DeadlineSixCompressed extends GroupKeyPartiallyCompressedEntryId(709) with Deadline.SixCompressed with ValueFullyCompressed
      case object DeadlineSevenCompressed extends GroupKeyPartiallyCompressedEntryId(710) with Deadline.SevenCompressed with ValueFullyCompressed
      case object DeadlineFullyCompressed extends GroupKeyPartiallyCompressedEntryId(711) with Deadline.FullyCompressed with ValueFullyCompressed
      case object DeadlineUncompressed extends GroupKeyPartiallyCompressedEntryId(712) with Deadline.Uncompressed with ValueFullyCompressed
    }

    sealed trait ValueUncompressed extends Value.Uncompressed with KeyPartiallyCompressed {
      override val valueOffsetOneCompressed: ValueOffset.OneCompressed = ValueUncompressed.ValueOffsetOneCompressed
      override val valueOffsetTwoCompressed: ValueOffset.TwoCompressed = ValueUncompressed.ValueOffsetTwoCompressed
      override val valueOffsetThreeCompressed: ValueOffset.ThreeCompressed = ValueUncompressed.ValueOffsetThreeCompressed
      override val valueOffsetUncompressed: ValueOffset.Uncompressed = ValueUncompressed.ValueOffsetUncompressed
    }
    object ValueUncompressed extends ValueUncompressed {

      sealed trait ValueOffsetOneCompressed extends ValueOffset.OneCompressed with ValueUncompressed {
        override val valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetOneCompressed.ValueLengthOneCompressed
        override val valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetOneCompressed.ValueLengthTwoCompressed
        override val valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetOneCompressed.ValueLengthThreeCompressed
        override val valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetOneCompressed.ValueLengthFullyCompressed
        override val valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetOneCompressed.ValueLengthUncompressed
      }
      object ValueOffsetOneCompressed extends ValueOffsetOneCompressed {
        sealed trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetOneCompressed {
          override val noDeadline: Deadline.NoDeadline = ValueLengthOneCompressed.NoDeadline
          override val deadlineOneCompressed: Deadline.OneCompressed = ValueLengthOneCompressed.DeadlineOneCompressed
          override val deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthOneCompressed.DeadlineTwoCompressed
          override val deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthOneCompressed.DeadlineThreeCompressed
          override val deadlineFourCompressed: Deadline.FourCompressed = ValueLengthOneCompressed.DeadlineFourCompressed
          override val deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthOneCompressed.DeadlineFiveCompressed
          override val deadlineSixCompressed: Deadline.SixCompressed = ValueLengthOneCompressed.DeadlineSixCompressed
          override val deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthOneCompressed.DeadlineSevenCompressed
          override val deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthOneCompressed.DeadlineFullyCompressed
          override val deadlineUncompressed: Deadline.Uncompressed = ValueLengthOneCompressed.DeadlineUncompressed
        }

        object ValueLengthOneCompressed extends ValueLengthOneCompressed {
          case object NoDeadline extends GroupKeyPartiallyCompressedEntryId(713) with Deadline.NoDeadline with ValueLengthOneCompressed
          case object DeadlineOneCompressed extends GroupKeyPartiallyCompressedEntryId(714) with Deadline.OneCompressed with ValueLengthOneCompressed
          case object DeadlineTwoCompressed extends GroupKeyPartiallyCompressedEntryId(715) with Deadline.TwoCompressed with ValueLengthOneCompressed
          case object DeadlineThreeCompressed extends GroupKeyPartiallyCompressedEntryId(716) with Deadline.ThreeCompressed with ValueLengthOneCompressed
          case object DeadlineFourCompressed extends GroupKeyPartiallyCompressedEntryId(717) with Deadline.FourCompressed with ValueLengthOneCompressed
          case object DeadlineFiveCompressed extends GroupKeyPartiallyCompressedEntryId(718) with Deadline.FiveCompressed with ValueLengthOneCompressed
          case object DeadlineSixCompressed extends GroupKeyPartiallyCompressedEntryId(719) with Deadline.SixCompressed with ValueLengthOneCompressed
          case object DeadlineSevenCompressed extends GroupKeyPartiallyCompressedEntryId(720) with Deadline.SevenCompressed with ValueLengthOneCompressed
          case object DeadlineFullyCompressed extends GroupKeyPartiallyCompressedEntryId(721) with Deadline.FullyCompressed with ValueLengthOneCompressed
          case object DeadlineUncompressed extends GroupKeyPartiallyCompressedEntryId(722) with Deadline.Uncompressed with ValueLengthOneCompressed
        }

        sealed trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetOneCompressed {
          override val noDeadline: Deadline.NoDeadline = ValueLengthTwoCompressed.NoDeadline
          override val deadlineOneCompressed: Deadline.OneCompressed = ValueLengthTwoCompressed.DeadlineOneCompressed
          override val deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthTwoCompressed.DeadlineTwoCompressed
          override val deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthTwoCompressed.DeadlineThreeCompressed
          override val deadlineFourCompressed: Deadline.FourCompressed = ValueLengthTwoCompressed.DeadlineFourCompressed
          override val deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthTwoCompressed.DeadlineFiveCompressed
          override val deadlineSixCompressed: Deadline.SixCompressed = ValueLengthTwoCompressed.DeadlineSixCompressed
          override val deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthTwoCompressed.DeadlineSevenCompressed
          override val deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthTwoCompressed.DeadlineFullyCompressed
          override val deadlineUncompressed: Deadline.Uncompressed = ValueLengthTwoCompressed.DeadlineUncompressed
        }

        object ValueLengthTwoCompressed extends ValueLengthTwoCompressed {
          case object NoDeadline extends GroupKeyPartiallyCompressedEntryId(723) with Deadline.NoDeadline with ValueLengthTwoCompressed
          case object DeadlineOneCompressed extends GroupKeyPartiallyCompressedEntryId(724) with Deadline.OneCompressed with ValueLengthTwoCompressed
          case object DeadlineTwoCompressed extends GroupKeyPartiallyCompressedEntryId(725) with Deadline.TwoCompressed with ValueLengthTwoCompressed
          case object DeadlineThreeCompressed extends GroupKeyPartiallyCompressedEntryId(726) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
          case object DeadlineFourCompressed extends GroupKeyPartiallyCompressedEntryId(727) with Deadline.FourCompressed with ValueLengthTwoCompressed
          case object DeadlineFiveCompressed extends GroupKeyPartiallyCompressedEntryId(728) with Deadline.FiveCompressed with ValueLengthTwoCompressed
          case object DeadlineSixCompressed extends GroupKeyPartiallyCompressedEntryId(729) with Deadline.SixCompressed with ValueLengthTwoCompressed
          case object DeadlineSevenCompressed extends GroupKeyPartiallyCompressedEntryId(730) with Deadline.SevenCompressed with ValueLengthTwoCompressed
          case object DeadlineFullyCompressed extends GroupKeyPartiallyCompressedEntryId(731) with Deadline.FullyCompressed with ValueLengthTwoCompressed
          case object DeadlineUncompressed extends GroupKeyPartiallyCompressedEntryId(732) with Deadline.Uncompressed with ValueLengthTwoCompressed
        }

        sealed trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetOneCompressed {
          override val noDeadline: Deadline.NoDeadline = ValueLengthThreeCompressed.NoDeadline
          override val deadlineOneCompressed: Deadline.OneCompressed = ValueLengthThreeCompressed.DeadlineOneCompressed
          override val deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthThreeCompressed.DeadlineTwoCompressed
          override val deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthThreeCompressed.DeadlineThreeCompressed
          override val deadlineFourCompressed: Deadline.FourCompressed = ValueLengthThreeCompressed.DeadlineFourCompressed
          override val deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthThreeCompressed.DeadlineFiveCompressed
          override val deadlineSixCompressed: Deadline.SixCompressed = ValueLengthThreeCompressed.DeadlineSixCompressed
          override val deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthThreeCompressed.DeadlineSevenCompressed
          override val deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthThreeCompressed.DeadlineFullyCompressed
          override val deadlineUncompressed: Deadline.Uncompressed = ValueLengthThreeCompressed.DeadlineUncompressed
        }

        object ValueLengthThreeCompressed extends ValueLengthThreeCompressed {
          case object NoDeadline extends GroupKeyPartiallyCompressedEntryId(733) with Deadline.NoDeadline with ValueLengthThreeCompressed
          case object DeadlineOneCompressed extends GroupKeyPartiallyCompressedEntryId(734) with Deadline.OneCompressed with ValueLengthThreeCompressed
          case object DeadlineTwoCompressed extends GroupKeyPartiallyCompressedEntryId(735) with Deadline.TwoCompressed with ValueLengthThreeCompressed
          case object DeadlineThreeCompressed extends GroupKeyPartiallyCompressedEntryId(736) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
          case object DeadlineFourCompressed extends GroupKeyPartiallyCompressedEntryId(737) with Deadline.FourCompressed with ValueLengthThreeCompressed
          case object DeadlineFiveCompressed extends GroupKeyPartiallyCompressedEntryId(738) with Deadline.FiveCompressed with ValueLengthThreeCompressed
          case object DeadlineSixCompressed extends GroupKeyPartiallyCompressedEntryId(739) with Deadline.SixCompressed with ValueLengthThreeCompressed
          case object DeadlineSevenCompressed extends GroupKeyPartiallyCompressedEntryId(740) with Deadline.SevenCompressed with ValueLengthThreeCompressed
          case object DeadlineFullyCompressed extends GroupKeyPartiallyCompressedEntryId(741) with Deadline.FullyCompressed with ValueLengthThreeCompressed
          case object DeadlineUncompressed extends GroupKeyPartiallyCompressedEntryId(742) with Deadline.Uncompressed with ValueLengthThreeCompressed
        }

        sealed trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetOneCompressed {
          override val noDeadline: Deadline.NoDeadline = ValueLengthFullyCompressed.NoDeadline
          override val deadlineOneCompressed: Deadline.OneCompressed = ValueLengthFullyCompressed.DeadlineOneCompressed
          override val deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthFullyCompressed.DeadlineTwoCompressed
          override val deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthFullyCompressed.DeadlineThreeCompressed
          override val deadlineFourCompressed: Deadline.FourCompressed = ValueLengthFullyCompressed.DeadlineFourCompressed
          override val deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthFullyCompressed.DeadlineFiveCompressed
          override val deadlineSixCompressed: Deadline.SixCompressed = ValueLengthFullyCompressed.DeadlineSixCompressed
          override val deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthFullyCompressed.DeadlineSevenCompressed
          override val deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthFullyCompressed.DeadlineFullyCompressed
          override val deadlineUncompressed: Deadline.Uncompressed = ValueLengthFullyCompressed.DeadlineUncompressed
        }

        object ValueLengthFullyCompressed extends ValueLengthFullyCompressed {
          case object NoDeadline extends GroupKeyPartiallyCompressedEntryId(743) with Deadline.NoDeadline with ValueLengthFullyCompressed
          case object DeadlineOneCompressed extends GroupKeyPartiallyCompressedEntryId(744) with Deadline.OneCompressed with ValueLengthFullyCompressed
          case object DeadlineTwoCompressed extends GroupKeyPartiallyCompressedEntryId(745) with Deadline.TwoCompressed with ValueLengthFullyCompressed
          case object DeadlineThreeCompressed extends GroupKeyPartiallyCompressedEntryId(746) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
          case object DeadlineFourCompressed extends GroupKeyPartiallyCompressedEntryId(747) with Deadline.FourCompressed with ValueLengthFullyCompressed
          case object DeadlineFiveCompressed extends GroupKeyPartiallyCompressedEntryId(748) with Deadline.FiveCompressed with ValueLengthFullyCompressed
          case object DeadlineSixCompressed extends GroupKeyPartiallyCompressedEntryId(749) with Deadline.SixCompressed with ValueLengthFullyCompressed
          case object DeadlineSevenCompressed extends GroupKeyPartiallyCompressedEntryId(750) with Deadline.SevenCompressed with ValueLengthFullyCompressed
          case object DeadlineFullyCompressed extends GroupKeyPartiallyCompressedEntryId(751) with Deadline.FullyCompressed with ValueLengthFullyCompressed
          case object DeadlineUncompressed extends GroupKeyPartiallyCompressedEntryId(752) with Deadline.Uncompressed with ValueLengthFullyCompressed
        }

        sealed trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetOneCompressed {
          override val noDeadline: Deadline.NoDeadline = ValueLengthUncompressed.NoDeadline
          override val deadlineOneCompressed: Deadline.OneCompressed = ValueLengthUncompressed.DeadlineOneCompressed
          override val deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthUncompressed.DeadlineTwoCompressed
          override val deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthUncompressed.DeadlineThreeCompressed
          override val deadlineFourCompressed: Deadline.FourCompressed = ValueLengthUncompressed.DeadlineFourCompressed
          override val deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthUncompressed.DeadlineFiveCompressed
          override val deadlineSixCompressed: Deadline.SixCompressed = ValueLengthUncompressed.DeadlineSixCompressed
          override val deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthUncompressed.DeadlineSevenCompressed
          override val deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthUncompressed.DeadlineFullyCompressed
          override val deadlineUncompressed: Deadline.Uncompressed = ValueLengthUncompressed.DeadlineUncompressed
        }

        object ValueLengthUncompressed extends ValueLengthUncompressed {
          case object NoDeadline extends GroupKeyPartiallyCompressedEntryId(753) with Deadline.NoDeadline with ValueLengthUncompressed
          case object DeadlineOneCompressed extends GroupKeyPartiallyCompressedEntryId(754) with Deadline.OneCompressed with ValueLengthUncompressed
          case object DeadlineTwoCompressed extends GroupKeyPartiallyCompressedEntryId(755) with Deadline.TwoCompressed with ValueLengthUncompressed
          case object DeadlineThreeCompressed extends GroupKeyPartiallyCompressedEntryId(756) with Deadline.ThreeCompressed with ValueLengthUncompressed
          case object DeadlineFourCompressed extends GroupKeyPartiallyCompressedEntryId(757) with Deadline.FourCompressed with ValueLengthUncompressed
          case object DeadlineFiveCompressed extends GroupKeyPartiallyCompressedEntryId(758) with Deadline.FiveCompressed with ValueLengthUncompressed
          case object DeadlineSixCompressed extends GroupKeyPartiallyCompressedEntryId(759) with Deadline.SixCompressed with ValueLengthUncompressed
          case object DeadlineSevenCompressed extends GroupKeyPartiallyCompressedEntryId(760) with Deadline.SevenCompressed with ValueLengthUncompressed
          case object DeadlineFullyCompressed extends GroupKeyPartiallyCompressedEntryId(761) with Deadline.FullyCompressed with ValueLengthUncompressed
          case object DeadlineUncompressed extends GroupKeyPartiallyCompressedEntryId(762) with Deadline.Uncompressed with ValueLengthUncompressed
        }
      }

      sealed trait ValueOffsetTwoCompressed extends ValueOffset.TwoCompressed with ValueUncompressed {
        override val valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetTwoCompressed.ValueLengthOneCompressed
        override val valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetTwoCompressed.ValueLengthTwoCompressed
        override val valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetTwoCompressed.ValueLengthThreeCompressed
        override val valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetTwoCompressed.ValueLengthFullyCompressed
        override val valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetTwoCompressed.ValueLengthUncompressed
      }
      object ValueOffsetTwoCompressed extends ValueOffsetTwoCompressed {
        sealed trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetTwoCompressed {
          override val noDeadline: Deadline.NoDeadline = ValueLengthOneCompressed.NoDeadline
          override val deadlineOneCompressed: Deadline.OneCompressed = ValueLengthOneCompressed.DeadlineOneCompressed
          override val deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthOneCompressed.DeadlineTwoCompressed
          override val deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthOneCompressed.DeadlineThreeCompressed
          override val deadlineFourCompressed: Deadline.FourCompressed = ValueLengthOneCompressed.DeadlineFourCompressed
          override val deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthOneCompressed.DeadlineFiveCompressed
          override val deadlineSixCompressed: Deadline.SixCompressed = ValueLengthOneCompressed.DeadlineSixCompressed
          override val deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthOneCompressed.DeadlineSevenCompressed
          override val deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthOneCompressed.DeadlineFullyCompressed
          override val deadlineUncompressed: Deadline.Uncompressed = ValueLengthOneCompressed.DeadlineUncompressed
        }

        object ValueLengthOneCompressed extends ValueLengthOneCompressed {
          case object NoDeadline extends GroupKeyPartiallyCompressedEntryId(763) with Deadline.NoDeadline with ValueLengthOneCompressed
          case object DeadlineOneCompressed extends GroupKeyPartiallyCompressedEntryId(764) with Deadline.OneCompressed with ValueLengthOneCompressed
          case object DeadlineTwoCompressed extends GroupKeyPartiallyCompressedEntryId(765) with Deadline.TwoCompressed with ValueLengthOneCompressed
          case object DeadlineThreeCompressed extends GroupKeyPartiallyCompressedEntryId(766) with Deadline.ThreeCompressed with ValueLengthOneCompressed
          case object DeadlineFourCompressed extends GroupKeyPartiallyCompressedEntryId(767) with Deadline.FourCompressed with ValueLengthOneCompressed
          case object DeadlineFiveCompressed extends GroupKeyPartiallyCompressedEntryId(768) with Deadline.FiveCompressed with ValueLengthOneCompressed
          case object DeadlineSixCompressed extends GroupKeyPartiallyCompressedEntryId(769) with Deadline.SixCompressed with ValueLengthOneCompressed
          case object DeadlineSevenCompressed extends GroupKeyPartiallyCompressedEntryId(770) with Deadline.SevenCompressed with ValueLengthOneCompressed
          case object DeadlineFullyCompressed extends GroupKeyPartiallyCompressedEntryId(771) with Deadline.FullyCompressed with ValueLengthOneCompressed
          case object DeadlineUncompressed extends GroupKeyPartiallyCompressedEntryId(772) with Deadline.Uncompressed with ValueLengthOneCompressed
        }

        sealed trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetTwoCompressed {
          override val noDeadline: Deadline.NoDeadline = ValueLengthTwoCompressed.NoDeadline
          override val deadlineOneCompressed: Deadline.OneCompressed = ValueLengthTwoCompressed.DeadlineOneCompressed
          override val deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthTwoCompressed.DeadlineTwoCompressed
          override val deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthTwoCompressed.DeadlineThreeCompressed
          override val deadlineFourCompressed: Deadline.FourCompressed = ValueLengthTwoCompressed.DeadlineFourCompressed
          override val deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthTwoCompressed.DeadlineFiveCompressed
          override val deadlineSixCompressed: Deadline.SixCompressed = ValueLengthTwoCompressed.DeadlineSixCompressed
          override val deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthTwoCompressed.DeadlineSevenCompressed
          override val deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthTwoCompressed.DeadlineFullyCompressed
          override val deadlineUncompressed: Deadline.Uncompressed = ValueLengthTwoCompressed.DeadlineUncompressed
        }

        object ValueLengthTwoCompressed extends ValueLengthTwoCompressed {
          case object NoDeadline extends GroupKeyPartiallyCompressedEntryId(773) with Deadline.NoDeadline with ValueLengthTwoCompressed
          case object DeadlineOneCompressed extends GroupKeyPartiallyCompressedEntryId(774) with Deadline.OneCompressed with ValueLengthTwoCompressed
          case object DeadlineTwoCompressed extends GroupKeyPartiallyCompressedEntryId(775) with Deadline.TwoCompressed with ValueLengthTwoCompressed
          case object DeadlineThreeCompressed extends GroupKeyPartiallyCompressedEntryId(776) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
          case object DeadlineFourCompressed extends GroupKeyPartiallyCompressedEntryId(777) with Deadline.FourCompressed with ValueLengthTwoCompressed
          case object DeadlineFiveCompressed extends GroupKeyPartiallyCompressedEntryId(778) with Deadline.FiveCompressed with ValueLengthTwoCompressed
          case object DeadlineSixCompressed extends GroupKeyPartiallyCompressedEntryId(779) with Deadline.SixCompressed with ValueLengthTwoCompressed
          case object DeadlineSevenCompressed extends GroupKeyPartiallyCompressedEntryId(780) with Deadline.SevenCompressed with ValueLengthTwoCompressed
          case object DeadlineFullyCompressed extends GroupKeyPartiallyCompressedEntryId(781) with Deadline.FullyCompressed with ValueLengthTwoCompressed
          case object DeadlineUncompressed extends GroupKeyPartiallyCompressedEntryId(782) with Deadline.Uncompressed with ValueLengthTwoCompressed
        }

        sealed trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetTwoCompressed {
          override val noDeadline: Deadline.NoDeadline = ValueLengthThreeCompressed.NoDeadline
          override val deadlineOneCompressed: Deadline.OneCompressed = ValueLengthThreeCompressed.DeadlineOneCompressed
          override val deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthThreeCompressed.DeadlineTwoCompressed
          override val deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthThreeCompressed.DeadlineThreeCompressed
          override val deadlineFourCompressed: Deadline.FourCompressed = ValueLengthThreeCompressed.DeadlineFourCompressed
          override val deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthThreeCompressed.DeadlineFiveCompressed
          override val deadlineSixCompressed: Deadline.SixCompressed = ValueLengthThreeCompressed.DeadlineSixCompressed
          override val deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthThreeCompressed.DeadlineSevenCompressed
          override val deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthThreeCompressed.DeadlineFullyCompressed
          override val deadlineUncompressed: Deadline.Uncompressed = ValueLengthThreeCompressed.DeadlineUncompressed
        }

        object ValueLengthThreeCompressed extends ValueLengthThreeCompressed {
          case object NoDeadline extends GroupKeyPartiallyCompressedEntryId(783) with Deadline.NoDeadline with ValueLengthThreeCompressed
          case object DeadlineOneCompressed extends GroupKeyPartiallyCompressedEntryId(784) with Deadline.OneCompressed with ValueLengthThreeCompressed
          case object DeadlineTwoCompressed extends GroupKeyPartiallyCompressedEntryId(785) with Deadline.TwoCompressed with ValueLengthThreeCompressed
          case object DeadlineThreeCompressed extends GroupKeyPartiallyCompressedEntryId(786) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
          case object DeadlineFourCompressed extends GroupKeyPartiallyCompressedEntryId(787) with Deadline.FourCompressed with ValueLengthThreeCompressed
          case object DeadlineFiveCompressed extends GroupKeyPartiallyCompressedEntryId(788) with Deadline.FiveCompressed with ValueLengthThreeCompressed
          case object DeadlineSixCompressed extends GroupKeyPartiallyCompressedEntryId(789) with Deadline.SixCompressed with ValueLengthThreeCompressed
          case object DeadlineSevenCompressed extends GroupKeyPartiallyCompressedEntryId(790) with Deadline.SevenCompressed with ValueLengthThreeCompressed
          case object DeadlineFullyCompressed extends GroupKeyPartiallyCompressedEntryId(791) with Deadline.FullyCompressed with ValueLengthThreeCompressed
          case object DeadlineUncompressed extends GroupKeyPartiallyCompressedEntryId(792) with Deadline.Uncompressed with ValueLengthThreeCompressed
        }

        sealed trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetTwoCompressed {
          override val noDeadline: Deadline.NoDeadline = ValueLengthFullyCompressed.NoDeadline
          override val deadlineOneCompressed: Deadline.OneCompressed = ValueLengthFullyCompressed.DeadlineOneCompressed
          override val deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthFullyCompressed.DeadlineTwoCompressed
          override val deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthFullyCompressed.DeadlineThreeCompressed
          override val deadlineFourCompressed: Deadline.FourCompressed = ValueLengthFullyCompressed.DeadlineFourCompressed
          override val deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthFullyCompressed.DeadlineFiveCompressed
          override val deadlineSixCompressed: Deadline.SixCompressed = ValueLengthFullyCompressed.DeadlineSixCompressed
          override val deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthFullyCompressed.DeadlineSevenCompressed
          override val deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthFullyCompressed.DeadlineFullyCompressed
          override val deadlineUncompressed: Deadline.Uncompressed = ValueLengthFullyCompressed.DeadlineUncompressed
        }

        object ValueLengthFullyCompressed extends ValueLengthFullyCompressed {
          case object NoDeadline extends GroupKeyPartiallyCompressedEntryId(793) with Deadline.NoDeadline with ValueLengthFullyCompressed
          case object DeadlineOneCompressed extends GroupKeyPartiallyCompressedEntryId(794) with Deadline.OneCompressed with ValueLengthFullyCompressed
          case object DeadlineTwoCompressed extends GroupKeyPartiallyCompressedEntryId(795) with Deadline.TwoCompressed with ValueLengthFullyCompressed
          case object DeadlineThreeCompressed extends GroupKeyPartiallyCompressedEntryId(796) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
          case object DeadlineFourCompressed extends GroupKeyPartiallyCompressedEntryId(797) with Deadline.FourCompressed with ValueLengthFullyCompressed
          case object DeadlineFiveCompressed extends GroupKeyPartiallyCompressedEntryId(798) with Deadline.FiveCompressed with ValueLengthFullyCompressed
          case object DeadlineSixCompressed extends GroupKeyPartiallyCompressedEntryId(799) with Deadline.SixCompressed with ValueLengthFullyCompressed
          case object DeadlineSevenCompressed extends GroupKeyPartiallyCompressedEntryId(800) with Deadline.SevenCompressed with ValueLengthFullyCompressed
          case object DeadlineFullyCompressed extends GroupKeyPartiallyCompressedEntryId(801) with Deadline.FullyCompressed with ValueLengthFullyCompressed
          case object DeadlineUncompressed extends GroupKeyPartiallyCompressedEntryId(802) with Deadline.Uncompressed with ValueLengthFullyCompressed
        }

        sealed trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetTwoCompressed {
          override val noDeadline: Deadline.NoDeadline = ValueLengthUncompressed.NoDeadline
          override val deadlineOneCompressed: Deadline.OneCompressed = ValueLengthUncompressed.DeadlineOneCompressed
          override val deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthUncompressed.DeadlineTwoCompressed
          override val deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthUncompressed.DeadlineThreeCompressed
          override val deadlineFourCompressed: Deadline.FourCompressed = ValueLengthUncompressed.DeadlineFourCompressed
          override val deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthUncompressed.DeadlineFiveCompressed
          override val deadlineSixCompressed: Deadline.SixCompressed = ValueLengthUncompressed.DeadlineSixCompressed
          override val deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthUncompressed.DeadlineSevenCompressed
          override val deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthUncompressed.DeadlineFullyCompressed
          override val deadlineUncompressed: Deadline.Uncompressed = ValueLengthUncompressed.DeadlineUncompressed
        }

        object ValueLengthUncompressed extends ValueLengthUncompressed {
          case object NoDeadline extends GroupKeyPartiallyCompressedEntryId(803) with Deadline.NoDeadline with ValueLengthUncompressed
          case object DeadlineOneCompressed extends GroupKeyPartiallyCompressedEntryId(804) with Deadline.OneCompressed with ValueLengthUncompressed
          case object DeadlineTwoCompressed extends GroupKeyPartiallyCompressedEntryId(805) with Deadline.TwoCompressed with ValueLengthUncompressed
          case object DeadlineThreeCompressed extends GroupKeyPartiallyCompressedEntryId(806) with Deadline.ThreeCompressed with ValueLengthUncompressed
          case object DeadlineFourCompressed extends GroupKeyPartiallyCompressedEntryId(807) with Deadline.FourCompressed with ValueLengthUncompressed
          case object DeadlineFiveCompressed extends GroupKeyPartiallyCompressedEntryId(808) with Deadline.FiveCompressed with ValueLengthUncompressed
          case object DeadlineSixCompressed extends GroupKeyPartiallyCompressedEntryId(809) with Deadline.SixCompressed with ValueLengthUncompressed
          case object DeadlineSevenCompressed extends GroupKeyPartiallyCompressedEntryId(810) with Deadline.SevenCompressed with ValueLengthUncompressed
          case object DeadlineFullyCompressed extends GroupKeyPartiallyCompressedEntryId(811) with Deadline.FullyCompressed with ValueLengthUncompressed
          case object DeadlineUncompressed extends GroupKeyPartiallyCompressedEntryId(812) with Deadline.Uncompressed with ValueLengthUncompressed
        }
      }

      sealed trait ValueOffsetThreeCompressed extends ValueOffset.ThreeCompressed with ValueUncompressed {
        override val valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetThreeCompressed.ValueLengthOneCompressed
        override val valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetThreeCompressed.ValueLengthTwoCompressed
        override val valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetThreeCompressed.ValueLengthThreeCompressed
        override val valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetThreeCompressed.ValueLengthFullyCompressed
        override val valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetThreeCompressed.ValueLengthUncompressed
      }
      object ValueOffsetThreeCompressed extends ValueOffsetThreeCompressed {
        sealed trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetThreeCompressed {
          override val noDeadline: Deadline.NoDeadline = ValueLengthOneCompressed.NoDeadline
          override val deadlineOneCompressed: Deadline.OneCompressed = ValueLengthOneCompressed.DeadlineOneCompressed
          override val deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthOneCompressed.DeadlineTwoCompressed
          override val deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthOneCompressed.DeadlineThreeCompressed
          override val deadlineFourCompressed: Deadline.FourCompressed = ValueLengthOneCompressed.DeadlineFourCompressed
          override val deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthOneCompressed.DeadlineFiveCompressed
          override val deadlineSixCompressed: Deadline.SixCompressed = ValueLengthOneCompressed.DeadlineSixCompressed
          override val deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthOneCompressed.DeadlineSevenCompressed
          override val deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthOneCompressed.DeadlineFullyCompressed
          override val deadlineUncompressed: Deadline.Uncompressed = ValueLengthOneCompressed.DeadlineUncompressed
        }

        object ValueLengthOneCompressed extends ValueLengthOneCompressed {
          case object NoDeadline extends GroupKeyPartiallyCompressedEntryId(813) with Deadline.NoDeadline with ValueLengthOneCompressed
          case object DeadlineOneCompressed extends GroupKeyPartiallyCompressedEntryId(814) with Deadline.OneCompressed with ValueLengthOneCompressed
          case object DeadlineTwoCompressed extends GroupKeyPartiallyCompressedEntryId(815) with Deadline.TwoCompressed with ValueLengthOneCompressed
          case object DeadlineThreeCompressed extends GroupKeyPartiallyCompressedEntryId(816) with Deadline.ThreeCompressed with ValueLengthOneCompressed
          case object DeadlineFourCompressed extends GroupKeyPartiallyCompressedEntryId(817) with Deadline.FourCompressed with ValueLengthOneCompressed
          case object DeadlineFiveCompressed extends GroupKeyPartiallyCompressedEntryId(818) with Deadline.FiveCompressed with ValueLengthOneCompressed
          case object DeadlineSixCompressed extends GroupKeyPartiallyCompressedEntryId(819) with Deadline.SixCompressed with ValueLengthOneCompressed
          case object DeadlineSevenCompressed extends GroupKeyPartiallyCompressedEntryId(820) with Deadline.SevenCompressed with ValueLengthOneCompressed
          case object DeadlineFullyCompressed extends GroupKeyPartiallyCompressedEntryId(821) with Deadline.FullyCompressed with ValueLengthOneCompressed
          case object DeadlineUncompressed extends GroupKeyPartiallyCompressedEntryId(822) with Deadline.Uncompressed with ValueLengthOneCompressed
        }

        sealed trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetThreeCompressed {
          override val noDeadline: Deadline.NoDeadline = ValueLengthTwoCompressed.NoDeadline
          override val deadlineOneCompressed: Deadline.OneCompressed = ValueLengthTwoCompressed.DeadlineOneCompressed
          override val deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthTwoCompressed.DeadlineTwoCompressed
          override val deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthTwoCompressed.DeadlineThreeCompressed
          override val deadlineFourCompressed: Deadline.FourCompressed = ValueLengthTwoCompressed.DeadlineFourCompressed
          override val deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthTwoCompressed.DeadlineFiveCompressed
          override val deadlineSixCompressed: Deadline.SixCompressed = ValueLengthTwoCompressed.DeadlineSixCompressed
          override val deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthTwoCompressed.DeadlineSevenCompressed
          override val deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthTwoCompressed.DeadlineFullyCompressed
          override val deadlineUncompressed: Deadline.Uncompressed = ValueLengthTwoCompressed.DeadlineUncompressed
        }

        object ValueLengthTwoCompressed extends ValueLengthTwoCompressed {
          case object NoDeadline extends GroupKeyPartiallyCompressedEntryId(823) with Deadline.NoDeadline with ValueLengthTwoCompressed
          case object DeadlineOneCompressed extends GroupKeyPartiallyCompressedEntryId(824) with Deadline.OneCompressed with ValueLengthTwoCompressed
          case object DeadlineTwoCompressed extends GroupKeyPartiallyCompressedEntryId(825) with Deadline.TwoCompressed with ValueLengthTwoCompressed
          case object DeadlineThreeCompressed extends GroupKeyPartiallyCompressedEntryId(826) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
          case object DeadlineFourCompressed extends GroupKeyPartiallyCompressedEntryId(827) with Deadline.FourCompressed with ValueLengthTwoCompressed
          case object DeadlineFiveCompressed extends GroupKeyPartiallyCompressedEntryId(828) with Deadline.FiveCompressed with ValueLengthTwoCompressed
          case object DeadlineSixCompressed extends GroupKeyPartiallyCompressedEntryId(829) with Deadline.SixCompressed with ValueLengthTwoCompressed
          case object DeadlineSevenCompressed extends GroupKeyPartiallyCompressedEntryId(830) with Deadline.SevenCompressed with ValueLengthTwoCompressed
          case object DeadlineFullyCompressed extends GroupKeyPartiallyCompressedEntryId(831) with Deadline.FullyCompressed with ValueLengthTwoCompressed
          case object DeadlineUncompressed extends GroupKeyPartiallyCompressedEntryId(832) with Deadline.Uncompressed with ValueLengthTwoCompressed
        }

        sealed trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetThreeCompressed {
          override val noDeadline: Deadline.NoDeadline = ValueLengthThreeCompressed.NoDeadline
          override val deadlineOneCompressed: Deadline.OneCompressed = ValueLengthThreeCompressed.DeadlineOneCompressed
          override val deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthThreeCompressed.DeadlineTwoCompressed
          override val deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthThreeCompressed.DeadlineThreeCompressed
          override val deadlineFourCompressed: Deadline.FourCompressed = ValueLengthThreeCompressed.DeadlineFourCompressed
          override val deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthThreeCompressed.DeadlineFiveCompressed
          override val deadlineSixCompressed: Deadline.SixCompressed = ValueLengthThreeCompressed.DeadlineSixCompressed
          override val deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthThreeCompressed.DeadlineSevenCompressed
          override val deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthThreeCompressed.DeadlineFullyCompressed
          override val deadlineUncompressed: Deadline.Uncompressed = ValueLengthThreeCompressed.DeadlineUncompressed
        }

        object ValueLengthThreeCompressed extends ValueLengthThreeCompressed {
          case object NoDeadline extends GroupKeyPartiallyCompressedEntryId(833) with Deadline.NoDeadline with ValueLengthThreeCompressed
          case object DeadlineOneCompressed extends GroupKeyPartiallyCompressedEntryId(834) with Deadline.OneCompressed with ValueLengthThreeCompressed
          case object DeadlineTwoCompressed extends GroupKeyPartiallyCompressedEntryId(835) with Deadline.TwoCompressed with ValueLengthThreeCompressed
          case object DeadlineThreeCompressed extends GroupKeyPartiallyCompressedEntryId(836) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
          case object DeadlineFourCompressed extends GroupKeyPartiallyCompressedEntryId(837) with Deadline.FourCompressed with ValueLengthThreeCompressed
          case object DeadlineFiveCompressed extends GroupKeyPartiallyCompressedEntryId(838) with Deadline.FiveCompressed with ValueLengthThreeCompressed
          case object DeadlineSixCompressed extends GroupKeyPartiallyCompressedEntryId(839) with Deadline.SixCompressed with ValueLengthThreeCompressed
          case object DeadlineSevenCompressed extends GroupKeyPartiallyCompressedEntryId(840) with Deadline.SevenCompressed with ValueLengthThreeCompressed
          case object DeadlineFullyCompressed extends GroupKeyPartiallyCompressedEntryId(841) with Deadline.FullyCompressed with ValueLengthThreeCompressed
          case object DeadlineUncompressed extends GroupKeyPartiallyCompressedEntryId(842) with Deadline.Uncompressed with ValueLengthThreeCompressed
        }

        sealed trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetThreeCompressed {
          override val noDeadline: Deadline.NoDeadline = ValueLengthFullyCompressed.NoDeadline
          override val deadlineOneCompressed: Deadline.OneCompressed = ValueLengthFullyCompressed.DeadlineOneCompressed
          override val deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthFullyCompressed.DeadlineTwoCompressed
          override val deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthFullyCompressed.DeadlineThreeCompressed
          override val deadlineFourCompressed: Deadline.FourCompressed = ValueLengthFullyCompressed.DeadlineFourCompressed
          override val deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthFullyCompressed.DeadlineFiveCompressed
          override val deadlineSixCompressed: Deadline.SixCompressed = ValueLengthFullyCompressed.DeadlineSixCompressed
          override val deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthFullyCompressed.DeadlineSevenCompressed
          override val deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthFullyCompressed.DeadlineFullyCompressed
          override val deadlineUncompressed: Deadline.Uncompressed = ValueLengthFullyCompressed.DeadlineUncompressed
        }

        object ValueLengthFullyCompressed extends ValueLengthFullyCompressed {
          case object NoDeadline extends GroupKeyPartiallyCompressedEntryId(843) with Deadline.NoDeadline with ValueLengthFullyCompressed
          case object DeadlineOneCompressed extends GroupKeyPartiallyCompressedEntryId(844) with Deadline.OneCompressed with ValueLengthFullyCompressed
          case object DeadlineTwoCompressed extends GroupKeyPartiallyCompressedEntryId(845) with Deadline.TwoCompressed with ValueLengthFullyCompressed
          case object DeadlineThreeCompressed extends GroupKeyPartiallyCompressedEntryId(846) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
          case object DeadlineFourCompressed extends GroupKeyPartiallyCompressedEntryId(847) with Deadline.FourCompressed with ValueLengthFullyCompressed
          case object DeadlineFiveCompressed extends GroupKeyPartiallyCompressedEntryId(848) with Deadline.FiveCompressed with ValueLengthFullyCompressed
          case object DeadlineSixCompressed extends GroupKeyPartiallyCompressedEntryId(849) with Deadline.SixCompressed with ValueLengthFullyCompressed
          case object DeadlineSevenCompressed extends GroupKeyPartiallyCompressedEntryId(850) with Deadline.SevenCompressed with ValueLengthFullyCompressed
          case object DeadlineFullyCompressed extends GroupKeyPartiallyCompressedEntryId(851) with Deadline.FullyCompressed with ValueLengthFullyCompressed
          case object DeadlineUncompressed extends GroupKeyPartiallyCompressedEntryId(852) with Deadline.Uncompressed with ValueLengthFullyCompressed
        }

        sealed trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetThreeCompressed {
          override val noDeadline: Deadline.NoDeadline = ValueLengthUncompressed.NoDeadline
          override val deadlineOneCompressed: Deadline.OneCompressed = ValueLengthUncompressed.DeadlineOneCompressed
          override val deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthUncompressed.DeadlineTwoCompressed
          override val deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthUncompressed.DeadlineThreeCompressed
          override val deadlineFourCompressed: Deadline.FourCompressed = ValueLengthUncompressed.DeadlineFourCompressed
          override val deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthUncompressed.DeadlineFiveCompressed
          override val deadlineSixCompressed: Deadline.SixCompressed = ValueLengthUncompressed.DeadlineSixCompressed
          override val deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthUncompressed.DeadlineSevenCompressed
          override val deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthUncompressed.DeadlineFullyCompressed
          override val deadlineUncompressed: Deadline.Uncompressed = ValueLengthUncompressed.DeadlineUncompressed
        }

        object ValueLengthUncompressed extends ValueLengthUncompressed {
          case object NoDeadline extends GroupKeyPartiallyCompressedEntryId(853) with Deadline.NoDeadline with ValueLengthUncompressed
          case object DeadlineOneCompressed extends GroupKeyPartiallyCompressedEntryId(854) with Deadline.OneCompressed with ValueLengthUncompressed
          case object DeadlineTwoCompressed extends GroupKeyPartiallyCompressedEntryId(855) with Deadline.TwoCompressed with ValueLengthUncompressed
          case object DeadlineThreeCompressed extends GroupKeyPartiallyCompressedEntryId(856) with Deadline.ThreeCompressed with ValueLengthUncompressed
          case object DeadlineFourCompressed extends GroupKeyPartiallyCompressedEntryId(857) with Deadline.FourCompressed with ValueLengthUncompressed
          case object DeadlineFiveCompressed extends GroupKeyPartiallyCompressedEntryId(858) with Deadline.FiveCompressed with ValueLengthUncompressed
          case object DeadlineSixCompressed extends GroupKeyPartiallyCompressedEntryId(859) with Deadline.SixCompressed with ValueLengthUncompressed
          case object DeadlineSevenCompressed extends GroupKeyPartiallyCompressedEntryId(860) with Deadline.SevenCompressed with ValueLengthUncompressed
          case object DeadlineFullyCompressed extends GroupKeyPartiallyCompressedEntryId(861) with Deadline.FullyCompressed with ValueLengthUncompressed
          case object DeadlineUncompressed extends GroupKeyPartiallyCompressedEntryId(862) with Deadline.Uncompressed with ValueLengthUncompressed
        }
      }

      sealed trait ValueOffsetUncompressed extends ValueOffset.Uncompressed with ValueUncompressed {
        override val valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetUncompressed.ValueLengthOneCompressed
        override val valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetUncompressed.ValueLengthTwoCompressed
        override val valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetUncompressed.ValueLengthThreeCompressed
        override val valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetUncompressed.ValueLengthFullyCompressed
        override val valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetUncompressed.ValueLengthUncompressed
      }
      object ValueOffsetUncompressed extends ValueOffsetUncompressed {
        sealed trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetUncompressed {
          override val noDeadline: Deadline.NoDeadline = ValueLengthOneCompressed.NoDeadline
          override val deadlineOneCompressed: Deadline.OneCompressed = ValueLengthOneCompressed.DeadlineOneCompressed
          override val deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthOneCompressed.DeadlineTwoCompressed
          override val deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthOneCompressed.DeadlineThreeCompressed
          override val deadlineFourCompressed: Deadline.FourCompressed = ValueLengthOneCompressed.DeadlineFourCompressed
          override val deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthOneCompressed.DeadlineFiveCompressed
          override val deadlineSixCompressed: Deadline.SixCompressed = ValueLengthOneCompressed.DeadlineSixCompressed
          override val deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthOneCompressed.DeadlineSevenCompressed
          override val deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthOneCompressed.DeadlineFullyCompressed
          override val deadlineUncompressed: Deadline.Uncompressed = ValueLengthOneCompressed.DeadlineUncompressed
        }

        object ValueLengthOneCompressed extends ValueLengthOneCompressed {
          case object NoDeadline extends GroupKeyPartiallyCompressedEntryId(863) with Deadline.NoDeadline with ValueLengthOneCompressed
          case object DeadlineOneCompressed extends GroupKeyPartiallyCompressedEntryId(864) with Deadline.OneCompressed with ValueLengthOneCompressed
          case object DeadlineTwoCompressed extends GroupKeyPartiallyCompressedEntryId(865) with Deadline.TwoCompressed with ValueLengthOneCompressed
          case object DeadlineThreeCompressed extends GroupKeyPartiallyCompressedEntryId(866) with Deadline.ThreeCompressed with ValueLengthOneCompressed
          case object DeadlineFourCompressed extends GroupKeyPartiallyCompressedEntryId(867) with Deadline.FourCompressed with ValueLengthOneCompressed
          case object DeadlineFiveCompressed extends GroupKeyPartiallyCompressedEntryId(868) with Deadline.FiveCompressed with ValueLengthOneCompressed
          case object DeadlineSixCompressed extends GroupKeyPartiallyCompressedEntryId(869) with Deadline.SixCompressed with ValueLengthOneCompressed
          case object DeadlineSevenCompressed extends GroupKeyPartiallyCompressedEntryId(870) with Deadline.SevenCompressed with ValueLengthOneCompressed
          case object DeadlineFullyCompressed extends GroupKeyPartiallyCompressedEntryId(871) with Deadline.FullyCompressed with ValueLengthOneCompressed
          case object DeadlineUncompressed extends GroupKeyPartiallyCompressedEntryId(872) with Deadline.Uncompressed with ValueLengthOneCompressed
        }

        sealed trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetUncompressed {
          override val noDeadline: Deadline.NoDeadline = ValueLengthTwoCompressed.NoDeadline
          override val deadlineOneCompressed: Deadline.OneCompressed = ValueLengthTwoCompressed.DeadlineOneCompressed
          override val deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthTwoCompressed.DeadlineTwoCompressed
          override val deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthTwoCompressed.DeadlineThreeCompressed
          override val deadlineFourCompressed: Deadline.FourCompressed = ValueLengthTwoCompressed.DeadlineFourCompressed
          override val deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthTwoCompressed.DeadlineFiveCompressed
          override val deadlineSixCompressed: Deadline.SixCompressed = ValueLengthTwoCompressed.DeadlineSixCompressed
          override val deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthTwoCompressed.DeadlineSevenCompressed
          override val deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthTwoCompressed.DeadlineFullyCompressed
          override val deadlineUncompressed: Deadline.Uncompressed = ValueLengthTwoCompressed.DeadlineUncompressed
        }

        object ValueLengthTwoCompressed extends ValueLengthTwoCompressed {
          case object NoDeadline extends GroupKeyPartiallyCompressedEntryId(873) with Deadline.NoDeadline with ValueLengthTwoCompressed
          case object DeadlineOneCompressed extends GroupKeyPartiallyCompressedEntryId(874) with Deadline.OneCompressed with ValueLengthTwoCompressed
          case object DeadlineTwoCompressed extends GroupKeyPartiallyCompressedEntryId(875) with Deadline.TwoCompressed with ValueLengthTwoCompressed
          case object DeadlineThreeCompressed extends GroupKeyPartiallyCompressedEntryId(876) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
          case object DeadlineFourCompressed extends GroupKeyPartiallyCompressedEntryId(877) with Deadline.FourCompressed with ValueLengthTwoCompressed
          case object DeadlineFiveCompressed extends GroupKeyPartiallyCompressedEntryId(878) with Deadline.FiveCompressed with ValueLengthTwoCompressed
          case object DeadlineSixCompressed extends GroupKeyPartiallyCompressedEntryId(879) with Deadline.SixCompressed with ValueLengthTwoCompressed
          case object DeadlineSevenCompressed extends GroupKeyPartiallyCompressedEntryId(880) with Deadline.SevenCompressed with ValueLengthTwoCompressed
          case object DeadlineFullyCompressed extends GroupKeyPartiallyCompressedEntryId(881) with Deadline.FullyCompressed with ValueLengthTwoCompressed
          case object DeadlineUncompressed extends GroupKeyPartiallyCompressedEntryId(882) with Deadline.Uncompressed with ValueLengthTwoCompressed
        }

        sealed trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetUncompressed {
          override val noDeadline: Deadline.NoDeadline = ValueLengthThreeCompressed.NoDeadline
          override val deadlineOneCompressed: Deadline.OneCompressed = ValueLengthThreeCompressed.DeadlineOneCompressed
          override val deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthThreeCompressed.DeadlineTwoCompressed
          override val deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthThreeCompressed.DeadlineThreeCompressed
          override val deadlineFourCompressed: Deadline.FourCompressed = ValueLengthThreeCompressed.DeadlineFourCompressed
          override val deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthThreeCompressed.DeadlineFiveCompressed
          override val deadlineSixCompressed: Deadline.SixCompressed = ValueLengthThreeCompressed.DeadlineSixCompressed
          override val deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthThreeCompressed.DeadlineSevenCompressed
          override val deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthThreeCompressed.DeadlineFullyCompressed
          override val deadlineUncompressed: Deadline.Uncompressed = ValueLengthThreeCompressed.DeadlineUncompressed
        }

        object ValueLengthThreeCompressed extends ValueLengthThreeCompressed {
          case object NoDeadline extends GroupKeyPartiallyCompressedEntryId(883) with Deadline.NoDeadline with ValueLengthThreeCompressed
          case object DeadlineOneCompressed extends GroupKeyPartiallyCompressedEntryId(884) with Deadline.OneCompressed with ValueLengthThreeCompressed
          case object DeadlineTwoCompressed extends GroupKeyPartiallyCompressedEntryId(885) with Deadline.TwoCompressed with ValueLengthThreeCompressed
          case object DeadlineThreeCompressed extends GroupKeyPartiallyCompressedEntryId(886) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
          case object DeadlineFourCompressed extends GroupKeyPartiallyCompressedEntryId(887) with Deadline.FourCompressed with ValueLengthThreeCompressed
          case object DeadlineFiveCompressed extends GroupKeyPartiallyCompressedEntryId(888) with Deadline.FiveCompressed with ValueLengthThreeCompressed
          case object DeadlineSixCompressed extends GroupKeyPartiallyCompressedEntryId(889) with Deadline.SixCompressed with ValueLengthThreeCompressed
          case object DeadlineSevenCompressed extends GroupKeyPartiallyCompressedEntryId(890) with Deadline.SevenCompressed with ValueLengthThreeCompressed
          case object DeadlineFullyCompressed extends GroupKeyPartiallyCompressedEntryId(891) with Deadline.FullyCompressed with ValueLengthThreeCompressed
          case object DeadlineUncompressed extends GroupKeyPartiallyCompressedEntryId(892) with Deadline.Uncompressed with ValueLengthThreeCompressed
        }

        sealed trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetUncompressed {
          override val noDeadline: Deadline.NoDeadline = ValueLengthFullyCompressed.NoDeadline
          override val deadlineOneCompressed: Deadline.OneCompressed = ValueLengthFullyCompressed.DeadlineOneCompressed
          override val deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthFullyCompressed.DeadlineTwoCompressed
          override val deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthFullyCompressed.DeadlineThreeCompressed
          override val deadlineFourCompressed: Deadline.FourCompressed = ValueLengthFullyCompressed.DeadlineFourCompressed
          override val deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthFullyCompressed.DeadlineFiveCompressed
          override val deadlineSixCompressed: Deadline.SixCompressed = ValueLengthFullyCompressed.DeadlineSixCompressed
          override val deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthFullyCompressed.DeadlineSevenCompressed
          override val deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthFullyCompressed.DeadlineFullyCompressed
          override val deadlineUncompressed: Deadline.Uncompressed = ValueLengthFullyCompressed.DeadlineUncompressed
        }

        object ValueLengthFullyCompressed extends ValueLengthFullyCompressed {
          case object NoDeadline extends GroupKeyPartiallyCompressedEntryId(893) with Deadline.NoDeadline with ValueLengthFullyCompressed
          case object DeadlineOneCompressed extends GroupKeyPartiallyCompressedEntryId(894) with Deadline.OneCompressed with ValueLengthFullyCompressed
          case object DeadlineTwoCompressed extends GroupKeyPartiallyCompressedEntryId(895) with Deadline.TwoCompressed with ValueLengthFullyCompressed
          case object DeadlineThreeCompressed extends GroupKeyPartiallyCompressedEntryId(896) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
          case object DeadlineFourCompressed extends GroupKeyPartiallyCompressedEntryId(897) with Deadline.FourCompressed with ValueLengthFullyCompressed
          case object DeadlineFiveCompressed extends GroupKeyPartiallyCompressedEntryId(898) with Deadline.FiveCompressed with ValueLengthFullyCompressed
          case object DeadlineSixCompressed extends GroupKeyPartiallyCompressedEntryId(899) with Deadline.SixCompressed with ValueLengthFullyCompressed
          case object DeadlineSevenCompressed extends GroupKeyPartiallyCompressedEntryId(900) with Deadline.SevenCompressed with ValueLengthFullyCompressed
          case object DeadlineFullyCompressed extends GroupKeyPartiallyCompressedEntryId(901) with Deadline.FullyCompressed with ValueLengthFullyCompressed
          case object DeadlineUncompressed extends GroupKeyPartiallyCompressedEntryId(902) with Deadline.Uncompressed with ValueLengthFullyCompressed
        }

        sealed trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetUncompressed {
          override val noDeadline: Deadline.NoDeadline = ValueLengthUncompressed.NoDeadline
          override val deadlineOneCompressed: Deadline.OneCompressed = ValueLengthUncompressed.DeadlineOneCompressed
          override val deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthUncompressed.DeadlineTwoCompressed
          override val deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthUncompressed.DeadlineThreeCompressed
          override val deadlineFourCompressed: Deadline.FourCompressed = ValueLengthUncompressed.DeadlineFourCompressed
          override val deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthUncompressed.DeadlineFiveCompressed
          override val deadlineSixCompressed: Deadline.SixCompressed = ValueLengthUncompressed.DeadlineSixCompressed
          override val deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthUncompressed.DeadlineSevenCompressed
          override val deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthUncompressed.DeadlineFullyCompressed
          override val deadlineUncompressed: Deadline.Uncompressed = ValueLengthUncompressed.DeadlineUncompressed
        }

        object ValueLengthUncompressed extends ValueLengthUncompressed {
          case object NoDeadline extends GroupKeyPartiallyCompressedEntryId(903) with Deadline.NoDeadline with ValueLengthUncompressed
          case object DeadlineOneCompressed extends GroupKeyPartiallyCompressedEntryId(904) with Deadline.OneCompressed with ValueLengthUncompressed
          case object DeadlineTwoCompressed extends GroupKeyPartiallyCompressedEntryId(905) with Deadline.TwoCompressed with ValueLengthUncompressed
          case object DeadlineThreeCompressed extends GroupKeyPartiallyCompressedEntryId(906) with Deadline.ThreeCompressed with ValueLengthUncompressed
          case object DeadlineFourCompressed extends GroupKeyPartiallyCompressedEntryId(907) with Deadline.FourCompressed with ValueLengthUncompressed
          case object DeadlineFiveCompressed extends GroupKeyPartiallyCompressedEntryId(908) with Deadline.FiveCompressed with ValueLengthUncompressed
          case object DeadlineSixCompressed extends GroupKeyPartiallyCompressedEntryId(909) with Deadline.SixCompressed with ValueLengthUncompressed
          case object DeadlineSevenCompressed extends GroupKeyPartiallyCompressedEntryId(910) with Deadline.SevenCompressed with ValueLengthUncompressed
          case object DeadlineFullyCompressed extends GroupKeyPartiallyCompressedEntryId(911) with Deadline.FullyCompressed with ValueLengthUncompressed
          case object DeadlineUncompressed extends GroupKeyPartiallyCompressedEntryId(912) with Deadline.Uncompressed with ValueLengthUncompressed
        }
      }
    }
  }

  def keyIdsList: List[GroupKeyPartiallyCompressedEntryId] = SealedList.list[GroupKeyPartiallyCompressedEntryId].sortBy(_.id)

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