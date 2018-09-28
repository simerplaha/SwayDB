
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
sealed abstract class PutKeyPartiallyCompressedEntryId(override val id: Int) extends EntryId(id)
object PutKeyPartiallyCompressedEntryId {

  def keyIdsList: List[PutKeyPartiallyCompressedEntryId] = SealedList.list[PutKeyPartiallyCompressedEntryId].sortBy(_.id)

  private val (headId, lastId) = keyIdsList ==> {
    keyIdsList =>
      (keyIdsList.head.id, keyIdsList.last.id)
  }

  def contains(id: Int): Option[Int] =
    if (id >= headId && id <= lastId)
      Some(id)
    else
      None

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
      case object NoDeadline extends PutKeyPartiallyCompressedEntryId(30) with Deadline.NoDeadline with NoValue
      case object DeadlineOneCompressed extends PutKeyPartiallyCompressedEntryId(31) with Deadline.OneCompressed with NoValue
      case object DeadlineTwoCompressed extends PutKeyPartiallyCompressedEntryId(32) with Deadline.TwoCompressed with NoValue
      case object DeadlineThreeCompressed extends PutKeyPartiallyCompressedEntryId(33) with Deadline.ThreeCompressed with NoValue
      case object DeadlineFourCompressed extends PutKeyPartiallyCompressedEntryId(34) with Deadline.FourCompressed with NoValue
      case object DeadlineFiveCompressed extends PutKeyPartiallyCompressedEntryId(35) with Deadline.FiveCompressed with NoValue
      case object DeadlineSixCompressed extends PutKeyPartiallyCompressedEntryId(36) with Deadline.SixCompressed with NoValue
      case object DeadlineSevenCompressed extends PutKeyPartiallyCompressedEntryId(37) with Deadline.SevenCompressed with NoValue
      case object DeadlineFullyCompressed extends PutKeyPartiallyCompressedEntryId(38) with Deadline.FullyCompressed with NoValue
      case object DeadlineUncompressed extends PutKeyPartiallyCompressedEntryId(39) with Deadline.Uncompressed with NoValue
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
      case object NoDeadline extends PutKeyPartiallyCompressedEntryId(40) with Deadline.NoDeadline with ValueFullyCompressed
      case object DeadlineOneCompressed extends PutKeyPartiallyCompressedEntryId(41) with Deadline.OneCompressed with ValueFullyCompressed
      case object DeadlineTwoCompressed extends PutKeyPartiallyCompressedEntryId(42) with Deadline.TwoCompressed with ValueFullyCompressed
      case object DeadlineThreeCompressed extends PutKeyPartiallyCompressedEntryId(43) with Deadline.ThreeCompressed with ValueFullyCompressed
      case object DeadlineFourCompressed extends PutKeyPartiallyCompressedEntryId(44) with Deadline.FourCompressed with ValueFullyCompressed
      case object DeadlineFiveCompressed extends PutKeyPartiallyCompressedEntryId(45) with Deadline.FiveCompressed with ValueFullyCompressed
      case object DeadlineSixCompressed extends PutKeyPartiallyCompressedEntryId(46) with Deadline.SixCompressed with ValueFullyCompressed
      case object DeadlineSevenCompressed extends PutKeyPartiallyCompressedEntryId(47) with Deadline.SevenCompressed with ValueFullyCompressed
      case object DeadlineFullyCompressed extends PutKeyPartiallyCompressedEntryId(48) with Deadline.FullyCompressed with ValueFullyCompressed
      case object DeadlineUncompressed extends PutKeyPartiallyCompressedEntryId(49) with Deadline.Uncompressed with ValueFullyCompressed
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
          case object NoDeadline extends PutKeyPartiallyCompressedEntryId(50) with Deadline.NoDeadline with ValueLengthOneCompressed
          case object DeadlineOneCompressed extends PutKeyPartiallyCompressedEntryId(51) with Deadline.OneCompressed with ValueLengthOneCompressed
          case object DeadlineTwoCompressed extends PutKeyPartiallyCompressedEntryId(52) with Deadline.TwoCompressed with ValueLengthOneCompressed
          case object DeadlineThreeCompressed extends PutKeyPartiallyCompressedEntryId(53) with Deadline.ThreeCompressed with ValueLengthOneCompressed
          case object DeadlineFourCompressed extends PutKeyPartiallyCompressedEntryId(54) with Deadline.FourCompressed with ValueLengthOneCompressed
          case object DeadlineFiveCompressed extends PutKeyPartiallyCompressedEntryId(55) with Deadline.FiveCompressed with ValueLengthOneCompressed
          case object DeadlineSixCompressed extends PutKeyPartiallyCompressedEntryId(56) with Deadline.SixCompressed with ValueLengthOneCompressed
          case object DeadlineSevenCompressed extends PutKeyPartiallyCompressedEntryId(57) with Deadline.SevenCompressed with ValueLengthOneCompressed
          case object DeadlineFullyCompressed extends PutKeyPartiallyCompressedEntryId(58) with Deadline.FullyCompressed with ValueLengthOneCompressed
          case object DeadlineUncompressed extends PutKeyPartiallyCompressedEntryId(59) with Deadline.Uncompressed with ValueLengthOneCompressed
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
          case object NoDeadline extends PutKeyPartiallyCompressedEntryId(60) with Deadline.NoDeadline with ValueLengthTwoCompressed
          case object DeadlineOneCompressed extends PutKeyPartiallyCompressedEntryId(61) with Deadline.OneCompressed with ValueLengthTwoCompressed
          case object DeadlineTwoCompressed extends PutKeyPartiallyCompressedEntryId(62) with Deadline.TwoCompressed with ValueLengthTwoCompressed
          case object DeadlineThreeCompressed extends PutKeyPartiallyCompressedEntryId(63) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
          case object DeadlineFourCompressed extends PutKeyPartiallyCompressedEntryId(64) with Deadline.FourCompressed with ValueLengthTwoCompressed
          case object DeadlineFiveCompressed extends PutKeyPartiallyCompressedEntryId(65) with Deadline.FiveCompressed with ValueLengthTwoCompressed
          case object DeadlineSixCompressed extends PutKeyPartiallyCompressedEntryId(66) with Deadline.SixCompressed with ValueLengthTwoCompressed
          case object DeadlineSevenCompressed extends PutKeyPartiallyCompressedEntryId(67) with Deadline.SevenCompressed with ValueLengthTwoCompressed
          case object DeadlineFullyCompressed extends PutKeyPartiallyCompressedEntryId(68) with Deadline.FullyCompressed with ValueLengthTwoCompressed
          case object DeadlineUncompressed extends PutKeyPartiallyCompressedEntryId(69) with Deadline.Uncompressed with ValueLengthTwoCompressed
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
          case object NoDeadline extends PutKeyPartiallyCompressedEntryId(70) with Deadline.NoDeadline with ValueLengthThreeCompressed
          case object DeadlineOneCompressed extends PutKeyPartiallyCompressedEntryId(71) with Deadline.OneCompressed with ValueLengthThreeCompressed
          case object DeadlineTwoCompressed extends PutKeyPartiallyCompressedEntryId(72) with Deadline.TwoCompressed with ValueLengthThreeCompressed
          case object DeadlineThreeCompressed extends PutKeyPartiallyCompressedEntryId(73) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
          case object DeadlineFourCompressed extends PutKeyPartiallyCompressedEntryId(74) with Deadline.FourCompressed with ValueLengthThreeCompressed
          case object DeadlineFiveCompressed extends PutKeyPartiallyCompressedEntryId(75) with Deadline.FiveCompressed with ValueLengthThreeCompressed
          case object DeadlineSixCompressed extends PutKeyPartiallyCompressedEntryId(76) with Deadline.SixCompressed with ValueLengthThreeCompressed
          case object DeadlineSevenCompressed extends PutKeyPartiallyCompressedEntryId(77) with Deadline.SevenCompressed with ValueLengthThreeCompressed
          case object DeadlineFullyCompressed extends PutKeyPartiallyCompressedEntryId(78) with Deadline.FullyCompressed with ValueLengthThreeCompressed
          case object DeadlineUncompressed extends PutKeyPartiallyCompressedEntryId(79) with Deadline.Uncompressed with ValueLengthThreeCompressed
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
          case object NoDeadline extends PutKeyPartiallyCompressedEntryId(80) with Deadline.NoDeadline with ValueLengthFullyCompressed
          case object DeadlineOneCompressed extends PutKeyPartiallyCompressedEntryId(81) with Deadline.OneCompressed with ValueLengthFullyCompressed
          case object DeadlineTwoCompressed extends PutKeyPartiallyCompressedEntryId(82) with Deadline.TwoCompressed with ValueLengthFullyCompressed
          case object DeadlineThreeCompressed extends PutKeyPartiallyCompressedEntryId(83) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
          case object DeadlineFourCompressed extends PutKeyPartiallyCompressedEntryId(84) with Deadline.FourCompressed with ValueLengthFullyCompressed
          case object DeadlineFiveCompressed extends PutKeyPartiallyCompressedEntryId(85) with Deadline.FiveCompressed with ValueLengthFullyCompressed
          case object DeadlineSixCompressed extends PutKeyPartiallyCompressedEntryId(86) with Deadline.SixCompressed with ValueLengthFullyCompressed
          case object DeadlineSevenCompressed extends PutKeyPartiallyCompressedEntryId(87) with Deadline.SevenCompressed with ValueLengthFullyCompressed
          case object DeadlineFullyCompressed extends PutKeyPartiallyCompressedEntryId(88) with Deadline.FullyCompressed with ValueLengthFullyCompressed
          case object DeadlineUncompressed extends PutKeyPartiallyCompressedEntryId(89) with Deadline.Uncompressed with ValueLengthFullyCompressed
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
          case object NoDeadline extends PutKeyPartiallyCompressedEntryId(90) with Deadline.NoDeadline with ValueLengthUncompressed
          case object DeadlineOneCompressed extends PutKeyPartiallyCompressedEntryId(91) with Deadline.OneCompressed with ValueLengthUncompressed
          case object DeadlineTwoCompressed extends PutKeyPartiallyCompressedEntryId(92) with Deadline.TwoCompressed with ValueLengthUncompressed
          case object DeadlineThreeCompressed extends PutKeyPartiallyCompressedEntryId(93) with Deadline.ThreeCompressed with ValueLengthUncompressed
          case object DeadlineFourCompressed extends PutKeyPartiallyCompressedEntryId(94) with Deadline.FourCompressed with ValueLengthUncompressed
          case object DeadlineFiveCompressed extends PutKeyPartiallyCompressedEntryId(95) with Deadline.FiveCompressed with ValueLengthUncompressed
          case object DeadlineSixCompressed extends PutKeyPartiallyCompressedEntryId(96) with Deadline.SixCompressed with ValueLengthUncompressed
          case object DeadlineSevenCompressed extends PutKeyPartiallyCompressedEntryId(97) with Deadline.SevenCompressed with ValueLengthUncompressed
          case object DeadlineFullyCompressed extends PutKeyPartiallyCompressedEntryId(98) with Deadline.FullyCompressed with ValueLengthUncompressed
          case object DeadlineUncompressed extends PutKeyPartiallyCompressedEntryId(99) with Deadline.Uncompressed with ValueLengthUncompressed
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
          case object NoDeadline extends PutKeyPartiallyCompressedEntryId(100) with Deadline.NoDeadline with ValueLengthOneCompressed
          case object DeadlineOneCompressed extends PutKeyPartiallyCompressedEntryId(101) with Deadline.OneCompressed with ValueLengthOneCompressed
          case object DeadlineTwoCompressed extends PutKeyPartiallyCompressedEntryId(102) with Deadline.TwoCompressed with ValueLengthOneCompressed
          case object DeadlineThreeCompressed extends PutKeyPartiallyCompressedEntryId(103) with Deadline.ThreeCompressed with ValueLengthOneCompressed
          case object DeadlineFourCompressed extends PutKeyPartiallyCompressedEntryId(104) with Deadline.FourCompressed with ValueLengthOneCompressed
          case object DeadlineFiveCompressed extends PutKeyPartiallyCompressedEntryId(105) with Deadline.FiveCompressed with ValueLengthOneCompressed
          case object DeadlineSixCompressed extends PutKeyPartiallyCompressedEntryId(106) with Deadline.SixCompressed with ValueLengthOneCompressed
          case object DeadlineSevenCompressed extends PutKeyPartiallyCompressedEntryId(107) with Deadline.SevenCompressed with ValueLengthOneCompressed
          case object DeadlineFullyCompressed extends PutKeyPartiallyCompressedEntryId(108) with Deadline.FullyCompressed with ValueLengthOneCompressed
          case object DeadlineUncompressed extends PutKeyPartiallyCompressedEntryId(109) with Deadline.Uncompressed with ValueLengthOneCompressed
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
          case object NoDeadline extends PutKeyPartiallyCompressedEntryId(110) with Deadline.NoDeadline with ValueLengthTwoCompressed
          case object DeadlineOneCompressed extends PutKeyPartiallyCompressedEntryId(111) with Deadline.OneCompressed with ValueLengthTwoCompressed
          case object DeadlineTwoCompressed extends PutKeyPartiallyCompressedEntryId(112) with Deadline.TwoCompressed with ValueLengthTwoCompressed
          case object DeadlineThreeCompressed extends PutKeyPartiallyCompressedEntryId(113) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
          case object DeadlineFourCompressed extends PutKeyPartiallyCompressedEntryId(114) with Deadline.FourCompressed with ValueLengthTwoCompressed
          case object DeadlineFiveCompressed extends PutKeyPartiallyCompressedEntryId(115) with Deadline.FiveCompressed with ValueLengthTwoCompressed
          case object DeadlineSixCompressed extends PutKeyPartiallyCompressedEntryId(116) with Deadline.SixCompressed with ValueLengthTwoCompressed
          case object DeadlineSevenCompressed extends PutKeyPartiallyCompressedEntryId(117) with Deadline.SevenCompressed with ValueLengthTwoCompressed
          case object DeadlineFullyCompressed extends PutKeyPartiallyCompressedEntryId(118) with Deadline.FullyCompressed with ValueLengthTwoCompressed
          case object DeadlineUncompressed extends PutKeyPartiallyCompressedEntryId(119) with Deadline.Uncompressed with ValueLengthTwoCompressed
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
          case object NoDeadline extends PutKeyPartiallyCompressedEntryId(120) with Deadline.NoDeadline with ValueLengthThreeCompressed
          case object DeadlineOneCompressed extends PutKeyPartiallyCompressedEntryId(121) with Deadline.OneCompressed with ValueLengthThreeCompressed
          case object DeadlineTwoCompressed extends PutKeyPartiallyCompressedEntryId(122) with Deadline.TwoCompressed with ValueLengthThreeCompressed
          case object DeadlineThreeCompressed extends PutKeyPartiallyCompressedEntryId(123) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
          case object DeadlineFourCompressed extends PutKeyPartiallyCompressedEntryId(124) with Deadline.FourCompressed with ValueLengthThreeCompressed
          case object DeadlineFiveCompressed extends PutKeyPartiallyCompressedEntryId(125) with Deadline.FiveCompressed with ValueLengthThreeCompressed
          case object DeadlineSixCompressed extends PutKeyPartiallyCompressedEntryId(126) with Deadline.SixCompressed with ValueLengthThreeCompressed
          case object DeadlineSevenCompressed extends PutKeyPartiallyCompressedEntryId(127) with Deadline.SevenCompressed with ValueLengthThreeCompressed
          case object DeadlineFullyCompressed extends PutKeyPartiallyCompressedEntryId(128) with Deadline.FullyCompressed with ValueLengthThreeCompressed
          case object DeadlineUncompressed extends PutKeyPartiallyCompressedEntryId(129) with Deadline.Uncompressed with ValueLengthThreeCompressed
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
          case object NoDeadline extends PutKeyPartiallyCompressedEntryId(130) with Deadline.NoDeadline with ValueLengthFullyCompressed
          case object DeadlineOneCompressed extends PutKeyPartiallyCompressedEntryId(131) with Deadline.OneCompressed with ValueLengthFullyCompressed
          case object DeadlineTwoCompressed extends PutKeyPartiallyCompressedEntryId(132) with Deadline.TwoCompressed with ValueLengthFullyCompressed
          case object DeadlineThreeCompressed extends PutKeyPartiallyCompressedEntryId(133) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
          case object DeadlineFourCompressed extends PutKeyPartiallyCompressedEntryId(134) with Deadline.FourCompressed with ValueLengthFullyCompressed
          case object DeadlineFiveCompressed extends PutKeyPartiallyCompressedEntryId(135) with Deadline.FiveCompressed with ValueLengthFullyCompressed
          case object DeadlineSixCompressed extends PutKeyPartiallyCompressedEntryId(136) with Deadline.SixCompressed with ValueLengthFullyCompressed
          case object DeadlineSevenCompressed extends PutKeyPartiallyCompressedEntryId(137) with Deadline.SevenCompressed with ValueLengthFullyCompressed
          case object DeadlineFullyCompressed extends PutKeyPartiallyCompressedEntryId(138) with Deadline.FullyCompressed with ValueLengthFullyCompressed
          case object DeadlineUncompressed extends PutKeyPartiallyCompressedEntryId(139) with Deadline.Uncompressed with ValueLengthFullyCompressed
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
          case object NoDeadline extends PutKeyPartiallyCompressedEntryId(140) with Deadline.NoDeadline with ValueLengthUncompressed
          case object DeadlineOneCompressed extends PutKeyPartiallyCompressedEntryId(141) with Deadline.OneCompressed with ValueLengthUncompressed
          case object DeadlineTwoCompressed extends PutKeyPartiallyCompressedEntryId(142) with Deadline.TwoCompressed with ValueLengthUncompressed
          case object DeadlineThreeCompressed extends PutKeyPartiallyCompressedEntryId(143) with Deadline.ThreeCompressed with ValueLengthUncompressed
          case object DeadlineFourCompressed extends PutKeyPartiallyCompressedEntryId(144) with Deadline.FourCompressed with ValueLengthUncompressed
          case object DeadlineFiveCompressed extends PutKeyPartiallyCompressedEntryId(145) with Deadline.FiveCompressed with ValueLengthUncompressed
          case object DeadlineSixCompressed extends PutKeyPartiallyCompressedEntryId(146) with Deadline.SixCompressed with ValueLengthUncompressed
          case object DeadlineSevenCompressed extends PutKeyPartiallyCompressedEntryId(147) with Deadline.SevenCompressed with ValueLengthUncompressed
          case object DeadlineFullyCompressed extends PutKeyPartiallyCompressedEntryId(148) with Deadline.FullyCompressed with ValueLengthUncompressed
          case object DeadlineUncompressed extends PutKeyPartiallyCompressedEntryId(149) with Deadline.Uncompressed with ValueLengthUncompressed
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
          case object NoDeadline extends PutKeyPartiallyCompressedEntryId(150) with Deadline.NoDeadline with ValueLengthOneCompressed
          case object DeadlineOneCompressed extends PutKeyPartiallyCompressedEntryId(151) with Deadline.OneCompressed with ValueLengthOneCompressed
          case object DeadlineTwoCompressed extends PutKeyPartiallyCompressedEntryId(152) with Deadline.TwoCompressed with ValueLengthOneCompressed
          case object DeadlineThreeCompressed extends PutKeyPartiallyCompressedEntryId(153) with Deadline.ThreeCompressed with ValueLengthOneCompressed
          case object DeadlineFourCompressed extends PutKeyPartiallyCompressedEntryId(154) with Deadline.FourCompressed with ValueLengthOneCompressed
          case object DeadlineFiveCompressed extends PutKeyPartiallyCompressedEntryId(155) with Deadline.FiveCompressed with ValueLengthOneCompressed
          case object DeadlineSixCompressed extends PutKeyPartiallyCompressedEntryId(156) with Deadline.SixCompressed with ValueLengthOneCompressed
          case object DeadlineSevenCompressed extends PutKeyPartiallyCompressedEntryId(157) with Deadline.SevenCompressed with ValueLengthOneCompressed
          case object DeadlineFullyCompressed extends PutKeyPartiallyCompressedEntryId(158) with Deadline.FullyCompressed with ValueLengthOneCompressed
          case object DeadlineUncompressed extends PutKeyPartiallyCompressedEntryId(159) with Deadline.Uncompressed with ValueLengthOneCompressed
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
          case object NoDeadline extends PutKeyPartiallyCompressedEntryId(160) with Deadline.NoDeadline with ValueLengthTwoCompressed
          case object DeadlineOneCompressed extends PutKeyPartiallyCompressedEntryId(161) with Deadline.OneCompressed with ValueLengthTwoCompressed
          case object DeadlineTwoCompressed extends PutKeyPartiallyCompressedEntryId(162) with Deadline.TwoCompressed with ValueLengthTwoCompressed
          case object DeadlineThreeCompressed extends PutKeyPartiallyCompressedEntryId(163) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
          case object DeadlineFourCompressed extends PutKeyPartiallyCompressedEntryId(164) with Deadline.FourCompressed with ValueLengthTwoCompressed
          case object DeadlineFiveCompressed extends PutKeyPartiallyCompressedEntryId(165) with Deadline.FiveCompressed with ValueLengthTwoCompressed
          case object DeadlineSixCompressed extends PutKeyPartiallyCompressedEntryId(166) with Deadline.SixCompressed with ValueLengthTwoCompressed
          case object DeadlineSevenCompressed extends PutKeyPartiallyCompressedEntryId(167) with Deadline.SevenCompressed with ValueLengthTwoCompressed
          case object DeadlineFullyCompressed extends PutKeyPartiallyCompressedEntryId(168) with Deadline.FullyCompressed with ValueLengthTwoCompressed
          case object DeadlineUncompressed extends PutKeyPartiallyCompressedEntryId(169) with Deadline.Uncompressed with ValueLengthTwoCompressed
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
          case object NoDeadline extends PutKeyPartiallyCompressedEntryId(170) with Deadline.NoDeadline with ValueLengthThreeCompressed
          case object DeadlineOneCompressed extends PutKeyPartiallyCompressedEntryId(171) with Deadline.OneCompressed with ValueLengthThreeCompressed
          case object DeadlineTwoCompressed extends PutKeyPartiallyCompressedEntryId(172) with Deadline.TwoCompressed with ValueLengthThreeCompressed
          case object DeadlineThreeCompressed extends PutKeyPartiallyCompressedEntryId(173) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
          case object DeadlineFourCompressed extends PutKeyPartiallyCompressedEntryId(174) with Deadline.FourCompressed with ValueLengthThreeCompressed
          case object DeadlineFiveCompressed extends PutKeyPartiallyCompressedEntryId(175) with Deadline.FiveCompressed with ValueLengthThreeCompressed
          case object DeadlineSixCompressed extends PutKeyPartiallyCompressedEntryId(176) with Deadline.SixCompressed with ValueLengthThreeCompressed
          case object DeadlineSevenCompressed extends PutKeyPartiallyCompressedEntryId(177) with Deadline.SevenCompressed with ValueLengthThreeCompressed
          case object DeadlineFullyCompressed extends PutKeyPartiallyCompressedEntryId(178) with Deadline.FullyCompressed with ValueLengthThreeCompressed
          case object DeadlineUncompressed extends PutKeyPartiallyCompressedEntryId(179) with Deadline.Uncompressed with ValueLengthThreeCompressed
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
          case object NoDeadline extends PutKeyPartiallyCompressedEntryId(180) with Deadline.NoDeadline with ValueLengthFullyCompressed
          case object DeadlineOneCompressed extends PutKeyPartiallyCompressedEntryId(181) with Deadline.OneCompressed with ValueLengthFullyCompressed
          case object DeadlineTwoCompressed extends PutKeyPartiallyCompressedEntryId(182) with Deadline.TwoCompressed with ValueLengthFullyCompressed
          case object DeadlineThreeCompressed extends PutKeyPartiallyCompressedEntryId(183) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
          case object DeadlineFourCompressed extends PutKeyPartiallyCompressedEntryId(184) with Deadline.FourCompressed with ValueLengthFullyCompressed
          case object DeadlineFiveCompressed extends PutKeyPartiallyCompressedEntryId(185) with Deadline.FiveCompressed with ValueLengthFullyCompressed
          case object DeadlineSixCompressed extends PutKeyPartiallyCompressedEntryId(186) with Deadline.SixCompressed with ValueLengthFullyCompressed
          case object DeadlineSevenCompressed extends PutKeyPartiallyCompressedEntryId(187) with Deadline.SevenCompressed with ValueLengthFullyCompressed
          case object DeadlineFullyCompressed extends PutKeyPartiallyCompressedEntryId(188) with Deadline.FullyCompressed with ValueLengthFullyCompressed
          case object DeadlineUncompressed extends PutKeyPartiallyCompressedEntryId(189) with Deadline.Uncompressed with ValueLengthFullyCompressed
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
          case object NoDeadline extends PutKeyPartiallyCompressedEntryId(190) with Deadline.NoDeadline with ValueLengthUncompressed
          case object DeadlineOneCompressed extends PutKeyPartiallyCompressedEntryId(191) with Deadline.OneCompressed with ValueLengthUncompressed
          case object DeadlineTwoCompressed extends PutKeyPartiallyCompressedEntryId(192) with Deadline.TwoCompressed with ValueLengthUncompressed
          case object DeadlineThreeCompressed extends PutKeyPartiallyCompressedEntryId(193) with Deadline.ThreeCompressed with ValueLengthUncompressed
          case object DeadlineFourCompressed extends PutKeyPartiallyCompressedEntryId(194) with Deadline.FourCompressed with ValueLengthUncompressed
          case object DeadlineFiveCompressed extends PutKeyPartiallyCompressedEntryId(195) with Deadline.FiveCompressed with ValueLengthUncompressed
          case object DeadlineSixCompressed extends PutKeyPartiallyCompressedEntryId(196) with Deadline.SixCompressed with ValueLengthUncompressed
          case object DeadlineSevenCompressed extends PutKeyPartiallyCompressedEntryId(197) with Deadline.SevenCompressed with ValueLengthUncompressed
          case object DeadlineFullyCompressed extends PutKeyPartiallyCompressedEntryId(198) with Deadline.FullyCompressed with ValueLengthUncompressed
          case object DeadlineUncompressed extends PutKeyPartiallyCompressedEntryId(199) with Deadline.Uncompressed with ValueLengthUncompressed
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
          case object NoDeadline extends PutKeyPartiallyCompressedEntryId(200) with Deadline.NoDeadline with ValueLengthOneCompressed
          case object DeadlineOneCompressed extends PutKeyPartiallyCompressedEntryId(201) with Deadline.OneCompressed with ValueLengthOneCompressed
          case object DeadlineTwoCompressed extends PutKeyPartiallyCompressedEntryId(202) with Deadline.TwoCompressed with ValueLengthOneCompressed
          case object DeadlineThreeCompressed extends PutKeyPartiallyCompressedEntryId(203) with Deadline.ThreeCompressed with ValueLengthOneCompressed
          case object DeadlineFourCompressed extends PutKeyPartiallyCompressedEntryId(204) with Deadline.FourCompressed with ValueLengthOneCompressed
          case object DeadlineFiveCompressed extends PutKeyPartiallyCompressedEntryId(205) with Deadline.FiveCompressed with ValueLengthOneCompressed
          case object DeadlineSixCompressed extends PutKeyPartiallyCompressedEntryId(206) with Deadline.SixCompressed with ValueLengthOneCompressed
          case object DeadlineSevenCompressed extends PutKeyPartiallyCompressedEntryId(207) with Deadline.SevenCompressed with ValueLengthOneCompressed
          case object DeadlineFullyCompressed extends PutKeyPartiallyCompressedEntryId(208) with Deadline.FullyCompressed with ValueLengthOneCompressed
          case object DeadlineUncompressed extends PutKeyPartiallyCompressedEntryId(209) with Deadline.Uncompressed with ValueLengthOneCompressed
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
          case object NoDeadline extends PutKeyPartiallyCompressedEntryId(210) with Deadline.NoDeadline with ValueLengthTwoCompressed
          case object DeadlineOneCompressed extends PutKeyPartiallyCompressedEntryId(211) with Deadline.OneCompressed with ValueLengthTwoCompressed
          case object DeadlineTwoCompressed extends PutKeyPartiallyCompressedEntryId(212) with Deadline.TwoCompressed with ValueLengthTwoCompressed
          case object DeadlineThreeCompressed extends PutKeyPartiallyCompressedEntryId(213) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
          case object DeadlineFourCompressed extends PutKeyPartiallyCompressedEntryId(214) with Deadline.FourCompressed with ValueLengthTwoCompressed
          case object DeadlineFiveCompressed extends PutKeyPartiallyCompressedEntryId(215) with Deadline.FiveCompressed with ValueLengthTwoCompressed
          case object DeadlineSixCompressed extends PutKeyPartiallyCompressedEntryId(216) with Deadline.SixCompressed with ValueLengthTwoCompressed
          case object DeadlineSevenCompressed extends PutKeyPartiallyCompressedEntryId(217) with Deadline.SevenCompressed with ValueLengthTwoCompressed
          case object DeadlineFullyCompressed extends PutKeyPartiallyCompressedEntryId(218) with Deadline.FullyCompressed with ValueLengthTwoCompressed
          case object DeadlineUncompressed extends PutKeyPartiallyCompressedEntryId(219) with Deadline.Uncompressed with ValueLengthTwoCompressed
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
          case object NoDeadline extends PutKeyPartiallyCompressedEntryId(220) with Deadline.NoDeadline with ValueLengthThreeCompressed
          case object DeadlineOneCompressed extends PutKeyPartiallyCompressedEntryId(221) with Deadline.OneCompressed with ValueLengthThreeCompressed
          case object DeadlineTwoCompressed extends PutKeyPartiallyCompressedEntryId(222) with Deadline.TwoCompressed with ValueLengthThreeCompressed
          case object DeadlineThreeCompressed extends PutKeyPartiallyCompressedEntryId(223) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
          case object DeadlineFourCompressed extends PutKeyPartiallyCompressedEntryId(224) with Deadline.FourCompressed with ValueLengthThreeCompressed
          case object DeadlineFiveCompressed extends PutKeyPartiallyCompressedEntryId(225) with Deadline.FiveCompressed with ValueLengthThreeCompressed
          case object DeadlineSixCompressed extends PutKeyPartiallyCompressedEntryId(226) with Deadline.SixCompressed with ValueLengthThreeCompressed
          case object DeadlineSevenCompressed extends PutKeyPartiallyCompressedEntryId(227) with Deadline.SevenCompressed with ValueLengthThreeCompressed
          case object DeadlineFullyCompressed extends PutKeyPartiallyCompressedEntryId(228) with Deadline.FullyCompressed with ValueLengthThreeCompressed
          case object DeadlineUncompressed extends PutKeyPartiallyCompressedEntryId(229) with Deadline.Uncompressed with ValueLengthThreeCompressed
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
          case object NoDeadline extends PutKeyPartiallyCompressedEntryId(230) with Deadline.NoDeadline with ValueLengthFullyCompressed
          case object DeadlineOneCompressed extends PutKeyPartiallyCompressedEntryId(231) with Deadline.OneCompressed with ValueLengthFullyCompressed
          case object DeadlineTwoCompressed extends PutKeyPartiallyCompressedEntryId(232) with Deadline.TwoCompressed with ValueLengthFullyCompressed
          case object DeadlineThreeCompressed extends PutKeyPartiallyCompressedEntryId(233) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
          case object DeadlineFourCompressed extends PutKeyPartiallyCompressedEntryId(234) with Deadline.FourCompressed with ValueLengthFullyCompressed
          case object DeadlineFiveCompressed extends PutKeyPartiallyCompressedEntryId(235) with Deadline.FiveCompressed with ValueLengthFullyCompressed
          case object DeadlineSixCompressed extends PutKeyPartiallyCompressedEntryId(236) with Deadline.SixCompressed with ValueLengthFullyCompressed
          case object DeadlineSevenCompressed extends PutKeyPartiallyCompressedEntryId(237) with Deadline.SevenCompressed with ValueLengthFullyCompressed
          case object DeadlineFullyCompressed extends PutKeyPartiallyCompressedEntryId(238) with Deadline.FullyCompressed with ValueLengthFullyCompressed
          case object DeadlineUncompressed extends PutKeyPartiallyCompressedEntryId(239) with Deadline.Uncompressed with ValueLengthFullyCompressed
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
          case object NoDeadline extends PutKeyPartiallyCompressedEntryId(240) with Deadline.NoDeadline with ValueLengthUncompressed
          case object DeadlineOneCompressed extends PutKeyPartiallyCompressedEntryId(241) with Deadline.OneCompressed with ValueLengthUncompressed
          case object DeadlineTwoCompressed extends PutKeyPartiallyCompressedEntryId(242) with Deadline.TwoCompressed with ValueLengthUncompressed
          case object DeadlineThreeCompressed extends PutKeyPartiallyCompressedEntryId(243) with Deadline.ThreeCompressed with ValueLengthUncompressed
          case object DeadlineFourCompressed extends PutKeyPartiallyCompressedEntryId(244) with Deadline.FourCompressed with ValueLengthUncompressed
          case object DeadlineFiveCompressed extends PutKeyPartiallyCompressedEntryId(245) with Deadline.FiveCompressed with ValueLengthUncompressed
          case object DeadlineSixCompressed extends PutKeyPartiallyCompressedEntryId(246) with Deadline.SixCompressed with ValueLengthUncompressed
          case object DeadlineSevenCompressed extends PutKeyPartiallyCompressedEntryId(247) with Deadline.SevenCompressed with ValueLengthUncompressed
          case object DeadlineFullyCompressed extends PutKeyPartiallyCompressedEntryId(248) with Deadline.FullyCompressed with ValueLengthUncompressed
          case object DeadlineUncompressed extends PutKeyPartiallyCompressedEntryId(249) with Deadline.Uncompressed with ValueLengthUncompressed
        }
      }
    }
  }
}