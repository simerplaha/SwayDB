
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
sealed abstract class UpdateFunctionKeyPartiallyCompressedEntryId(override val id: Int) extends EntryId(id)
object UpdateFunctionKeyPartiallyCompressedEntryId {

  def keyIdsList: List[UpdateFunctionKeyPartiallyCompressedEntryId] = SealedList.list[UpdateFunctionKeyPartiallyCompressedEntryId].sortBy(_.id)

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
      case object NoDeadline extends UpdateFunctionKeyPartiallyCompressedEntryId(2682) with Deadline.NoDeadline with NoValue
      case object DeadlineOneCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2683) with Deadline.OneCompressed with NoValue
      case object DeadlineTwoCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2684) with Deadline.TwoCompressed with NoValue
      case object DeadlineThreeCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2685) with Deadline.ThreeCompressed with NoValue
      case object DeadlineFourCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2686) with Deadline.FourCompressed with NoValue
      case object DeadlineFiveCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2687) with Deadline.FiveCompressed with NoValue
      case object DeadlineSixCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2688) with Deadline.SixCompressed with NoValue
      case object DeadlineSevenCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2689) with Deadline.SevenCompressed with NoValue
      case object DeadlineFullyCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2690) with Deadline.FullyCompressed with NoValue
      case object DeadlineUncompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2691) with Deadline.Uncompressed with NoValue
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
      case object NoDeadline extends UpdateFunctionKeyPartiallyCompressedEntryId(2692) with Deadline.NoDeadline with ValueFullyCompressed
      case object DeadlineOneCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2693) with Deadline.OneCompressed with ValueFullyCompressed
      case object DeadlineTwoCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2694) with Deadline.TwoCompressed with ValueFullyCompressed
      case object DeadlineThreeCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2695) with Deadline.ThreeCompressed with ValueFullyCompressed
      case object DeadlineFourCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2696) with Deadline.FourCompressed with ValueFullyCompressed
      case object DeadlineFiveCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2697) with Deadline.FiveCompressed with ValueFullyCompressed
      case object DeadlineSixCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2698) with Deadline.SixCompressed with ValueFullyCompressed
      case object DeadlineSevenCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2699) with Deadline.SevenCompressed with ValueFullyCompressed
      case object DeadlineFullyCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2700) with Deadline.FullyCompressed with ValueFullyCompressed
      case object DeadlineUncompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2701) with Deadline.Uncompressed with ValueFullyCompressed
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
          case object NoDeadline extends UpdateFunctionKeyPartiallyCompressedEntryId(2702) with Deadline.NoDeadline with ValueLengthOneCompressed
          case object DeadlineOneCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2703) with Deadline.OneCompressed with ValueLengthOneCompressed
          case object DeadlineTwoCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2704) with Deadline.TwoCompressed with ValueLengthOneCompressed
          case object DeadlineThreeCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2705) with Deadline.ThreeCompressed with ValueLengthOneCompressed
          case object DeadlineFourCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2706) with Deadline.FourCompressed with ValueLengthOneCompressed
          case object DeadlineFiveCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2707) with Deadline.FiveCompressed with ValueLengthOneCompressed
          case object DeadlineSixCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2708) with Deadline.SixCompressed with ValueLengthOneCompressed
          case object DeadlineSevenCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2709) with Deadline.SevenCompressed with ValueLengthOneCompressed
          case object DeadlineFullyCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2710) with Deadline.FullyCompressed with ValueLengthOneCompressed
          case object DeadlineUncompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2711) with Deadline.Uncompressed with ValueLengthOneCompressed
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
          case object NoDeadline extends UpdateFunctionKeyPartiallyCompressedEntryId(2712) with Deadline.NoDeadline with ValueLengthTwoCompressed
          case object DeadlineOneCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2713) with Deadline.OneCompressed with ValueLengthTwoCompressed
          case object DeadlineTwoCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2714) with Deadline.TwoCompressed with ValueLengthTwoCompressed
          case object DeadlineThreeCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2715) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
          case object DeadlineFourCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2716) with Deadline.FourCompressed with ValueLengthTwoCompressed
          case object DeadlineFiveCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2717) with Deadline.FiveCompressed with ValueLengthTwoCompressed
          case object DeadlineSixCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2718) with Deadline.SixCompressed with ValueLengthTwoCompressed
          case object DeadlineSevenCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2719) with Deadline.SevenCompressed with ValueLengthTwoCompressed
          case object DeadlineFullyCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2720) with Deadline.FullyCompressed with ValueLengthTwoCompressed
          case object DeadlineUncompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2721) with Deadline.Uncompressed with ValueLengthTwoCompressed
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
          case object NoDeadline extends UpdateFunctionKeyPartiallyCompressedEntryId(2722) with Deadline.NoDeadline with ValueLengthThreeCompressed
          case object DeadlineOneCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2723) with Deadline.OneCompressed with ValueLengthThreeCompressed
          case object DeadlineTwoCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2724) with Deadline.TwoCompressed with ValueLengthThreeCompressed
          case object DeadlineThreeCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2725) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
          case object DeadlineFourCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2726) with Deadline.FourCompressed with ValueLengthThreeCompressed
          case object DeadlineFiveCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2727) with Deadline.FiveCompressed with ValueLengthThreeCompressed
          case object DeadlineSixCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2728) with Deadline.SixCompressed with ValueLengthThreeCompressed
          case object DeadlineSevenCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2729) with Deadline.SevenCompressed with ValueLengthThreeCompressed
          case object DeadlineFullyCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2730) with Deadline.FullyCompressed with ValueLengthThreeCompressed
          case object DeadlineUncompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2731) with Deadline.Uncompressed with ValueLengthThreeCompressed
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
          case object NoDeadline extends UpdateFunctionKeyPartiallyCompressedEntryId(2732) with Deadline.NoDeadline with ValueLengthFullyCompressed
          case object DeadlineOneCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2733) with Deadline.OneCompressed with ValueLengthFullyCompressed
          case object DeadlineTwoCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2734) with Deadline.TwoCompressed with ValueLengthFullyCompressed
          case object DeadlineThreeCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2735) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
          case object DeadlineFourCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2736) with Deadline.FourCompressed with ValueLengthFullyCompressed
          case object DeadlineFiveCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2737) with Deadline.FiveCompressed with ValueLengthFullyCompressed
          case object DeadlineSixCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2738) with Deadline.SixCompressed with ValueLengthFullyCompressed
          case object DeadlineSevenCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2739) with Deadline.SevenCompressed with ValueLengthFullyCompressed
          case object DeadlineFullyCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2740) with Deadline.FullyCompressed with ValueLengthFullyCompressed
          case object DeadlineUncompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2741) with Deadline.Uncompressed with ValueLengthFullyCompressed
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
          case object NoDeadline extends UpdateFunctionKeyPartiallyCompressedEntryId(2742) with Deadline.NoDeadline with ValueLengthUncompressed
          case object DeadlineOneCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2743) with Deadline.OneCompressed with ValueLengthUncompressed
          case object DeadlineTwoCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2744) with Deadline.TwoCompressed with ValueLengthUncompressed
          case object DeadlineThreeCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2745) with Deadline.ThreeCompressed with ValueLengthUncompressed
          case object DeadlineFourCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2746) with Deadline.FourCompressed with ValueLengthUncompressed
          case object DeadlineFiveCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2747) with Deadline.FiveCompressed with ValueLengthUncompressed
          case object DeadlineSixCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2748) with Deadline.SixCompressed with ValueLengthUncompressed
          case object DeadlineSevenCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2749) with Deadline.SevenCompressed with ValueLengthUncompressed
          case object DeadlineFullyCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2750) with Deadline.FullyCompressed with ValueLengthUncompressed
          case object DeadlineUncompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2751) with Deadline.Uncompressed with ValueLengthUncompressed
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
          case object NoDeadline extends UpdateFunctionKeyPartiallyCompressedEntryId(2752) with Deadline.NoDeadline with ValueLengthOneCompressed
          case object DeadlineOneCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2753) with Deadline.OneCompressed with ValueLengthOneCompressed
          case object DeadlineTwoCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2754) with Deadline.TwoCompressed with ValueLengthOneCompressed
          case object DeadlineThreeCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2755) with Deadline.ThreeCompressed with ValueLengthOneCompressed
          case object DeadlineFourCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2756) with Deadline.FourCompressed with ValueLengthOneCompressed
          case object DeadlineFiveCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2757) with Deadline.FiveCompressed with ValueLengthOneCompressed
          case object DeadlineSixCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2758) with Deadline.SixCompressed with ValueLengthOneCompressed
          case object DeadlineSevenCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2759) with Deadline.SevenCompressed with ValueLengthOneCompressed
          case object DeadlineFullyCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2760) with Deadline.FullyCompressed with ValueLengthOneCompressed
          case object DeadlineUncompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2761) with Deadline.Uncompressed with ValueLengthOneCompressed
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
          case object NoDeadline extends UpdateFunctionKeyPartiallyCompressedEntryId(2762) with Deadline.NoDeadline with ValueLengthTwoCompressed
          case object DeadlineOneCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2763) with Deadline.OneCompressed with ValueLengthTwoCompressed
          case object DeadlineTwoCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2764) with Deadline.TwoCompressed with ValueLengthTwoCompressed
          case object DeadlineThreeCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2765) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
          case object DeadlineFourCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2766) with Deadline.FourCompressed with ValueLengthTwoCompressed
          case object DeadlineFiveCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2767) with Deadline.FiveCompressed with ValueLengthTwoCompressed
          case object DeadlineSixCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2768) with Deadline.SixCompressed with ValueLengthTwoCompressed
          case object DeadlineSevenCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2769) with Deadline.SevenCompressed with ValueLengthTwoCompressed
          case object DeadlineFullyCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2770) with Deadline.FullyCompressed with ValueLengthTwoCompressed
          case object DeadlineUncompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2771) with Deadline.Uncompressed with ValueLengthTwoCompressed
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
          case object NoDeadline extends UpdateFunctionKeyPartiallyCompressedEntryId(2772) with Deadline.NoDeadline with ValueLengthThreeCompressed
          case object DeadlineOneCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2773) with Deadline.OneCompressed with ValueLengthThreeCompressed
          case object DeadlineTwoCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2774) with Deadline.TwoCompressed with ValueLengthThreeCompressed
          case object DeadlineThreeCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2775) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
          case object DeadlineFourCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2776) with Deadline.FourCompressed with ValueLengthThreeCompressed
          case object DeadlineFiveCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2777) with Deadline.FiveCompressed with ValueLengthThreeCompressed
          case object DeadlineSixCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2778) with Deadline.SixCompressed with ValueLengthThreeCompressed
          case object DeadlineSevenCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2779) with Deadline.SevenCompressed with ValueLengthThreeCompressed
          case object DeadlineFullyCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2780) with Deadline.FullyCompressed with ValueLengthThreeCompressed
          case object DeadlineUncompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2781) with Deadline.Uncompressed with ValueLengthThreeCompressed
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
          case object NoDeadline extends UpdateFunctionKeyPartiallyCompressedEntryId(2782) with Deadline.NoDeadline with ValueLengthFullyCompressed
          case object DeadlineOneCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2783) with Deadline.OneCompressed with ValueLengthFullyCompressed
          case object DeadlineTwoCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2784) with Deadline.TwoCompressed with ValueLengthFullyCompressed
          case object DeadlineThreeCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2785) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
          case object DeadlineFourCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2786) with Deadline.FourCompressed with ValueLengthFullyCompressed
          case object DeadlineFiveCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2787) with Deadline.FiveCompressed with ValueLengthFullyCompressed
          case object DeadlineSixCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2788) with Deadline.SixCompressed with ValueLengthFullyCompressed
          case object DeadlineSevenCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2789) with Deadline.SevenCompressed with ValueLengthFullyCompressed
          case object DeadlineFullyCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2790) with Deadline.FullyCompressed with ValueLengthFullyCompressed
          case object DeadlineUncompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2791) with Deadline.Uncompressed with ValueLengthFullyCompressed
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
          case object NoDeadline extends UpdateFunctionKeyPartiallyCompressedEntryId(2792) with Deadline.NoDeadline with ValueLengthUncompressed
          case object DeadlineOneCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2793) with Deadline.OneCompressed with ValueLengthUncompressed
          case object DeadlineTwoCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2794) with Deadline.TwoCompressed with ValueLengthUncompressed
          case object DeadlineThreeCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2795) with Deadline.ThreeCompressed with ValueLengthUncompressed
          case object DeadlineFourCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2796) with Deadline.FourCompressed with ValueLengthUncompressed
          case object DeadlineFiveCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2797) with Deadline.FiveCompressed with ValueLengthUncompressed
          case object DeadlineSixCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2798) with Deadline.SixCompressed with ValueLengthUncompressed
          case object DeadlineSevenCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2799) with Deadline.SevenCompressed with ValueLengthUncompressed
          case object DeadlineFullyCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2800) with Deadline.FullyCompressed with ValueLengthUncompressed
          case object DeadlineUncompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2801) with Deadline.Uncompressed with ValueLengthUncompressed
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
          case object NoDeadline extends UpdateFunctionKeyPartiallyCompressedEntryId(2802) with Deadline.NoDeadline with ValueLengthOneCompressed
          case object DeadlineOneCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2803) with Deadline.OneCompressed with ValueLengthOneCompressed
          case object DeadlineTwoCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2804) with Deadline.TwoCompressed with ValueLengthOneCompressed
          case object DeadlineThreeCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2805) with Deadline.ThreeCompressed with ValueLengthOneCompressed
          case object DeadlineFourCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2806) with Deadline.FourCompressed with ValueLengthOneCompressed
          case object DeadlineFiveCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2807) with Deadline.FiveCompressed with ValueLengthOneCompressed
          case object DeadlineSixCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2808) with Deadline.SixCompressed with ValueLengthOneCompressed
          case object DeadlineSevenCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2809) with Deadline.SevenCompressed with ValueLengthOneCompressed
          case object DeadlineFullyCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2810) with Deadline.FullyCompressed with ValueLengthOneCompressed
          case object DeadlineUncompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2811) with Deadline.Uncompressed with ValueLengthOneCompressed
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
          case object NoDeadline extends UpdateFunctionKeyPartiallyCompressedEntryId(2812) with Deadline.NoDeadline with ValueLengthTwoCompressed
          case object DeadlineOneCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2813) with Deadline.OneCompressed with ValueLengthTwoCompressed
          case object DeadlineTwoCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2814) with Deadline.TwoCompressed with ValueLengthTwoCompressed
          case object DeadlineThreeCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2815) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
          case object DeadlineFourCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2816) with Deadline.FourCompressed with ValueLengthTwoCompressed
          case object DeadlineFiveCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2817) with Deadline.FiveCompressed with ValueLengthTwoCompressed
          case object DeadlineSixCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2818) with Deadline.SixCompressed with ValueLengthTwoCompressed
          case object DeadlineSevenCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2819) with Deadline.SevenCompressed with ValueLengthTwoCompressed
          case object DeadlineFullyCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2820) with Deadline.FullyCompressed with ValueLengthTwoCompressed
          case object DeadlineUncompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2821) with Deadline.Uncompressed with ValueLengthTwoCompressed
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
          case object NoDeadline extends UpdateFunctionKeyPartiallyCompressedEntryId(2822) with Deadline.NoDeadline with ValueLengthThreeCompressed
          case object DeadlineOneCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2823) with Deadline.OneCompressed with ValueLengthThreeCompressed
          case object DeadlineTwoCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2824) with Deadline.TwoCompressed with ValueLengthThreeCompressed
          case object DeadlineThreeCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2825) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
          case object DeadlineFourCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2826) with Deadline.FourCompressed with ValueLengthThreeCompressed
          case object DeadlineFiveCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2827) with Deadline.FiveCompressed with ValueLengthThreeCompressed
          case object DeadlineSixCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2828) with Deadline.SixCompressed with ValueLengthThreeCompressed
          case object DeadlineSevenCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2829) with Deadline.SevenCompressed with ValueLengthThreeCompressed
          case object DeadlineFullyCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2830) with Deadline.FullyCompressed with ValueLengthThreeCompressed
          case object DeadlineUncompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2831) with Deadline.Uncompressed with ValueLengthThreeCompressed
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
          case object NoDeadline extends UpdateFunctionKeyPartiallyCompressedEntryId(2832) with Deadline.NoDeadline with ValueLengthFullyCompressed
          case object DeadlineOneCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2833) with Deadline.OneCompressed with ValueLengthFullyCompressed
          case object DeadlineTwoCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2834) with Deadline.TwoCompressed with ValueLengthFullyCompressed
          case object DeadlineThreeCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2835) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
          case object DeadlineFourCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2836) with Deadline.FourCompressed with ValueLengthFullyCompressed
          case object DeadlineFiveCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2837) with Deadline.FiveCompressed with ValueLengthFullyCompressed
          case object DeadlineSixCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2838) with Deadline.SixCompressed with ValueLengthFullyCompressed
          case object DeadlineSevenCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2839) with Deadline.SevenCompressed with ValueLengthFullyCompressed
          case object DeadlineFullyCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2840) with Deadline.FullyCompressed with ValueLengthFullyCompressed
          case object DeadlineUncompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2841) with Deadline.Uncompressed with ValueLengthFullyCompressed
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
          case object NoDeadline extends UpdateFunctionKeyPartiallyCompressedEntryId(2842) with Deadline.NoDeadline with ValueLengthUncompressed
          case object DeadlineOneCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2843) with Deadline.OneCompressed with ValueLengthUncompressed
          case object DeadlineTwoCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2844) with Deadline.TwoCompressed with ValueLengthUncompressed
          case object DeadlineThreeCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2845) with Deadline.ThreeCompressed with ValueLengthUncompressed
          case object DeadlineFourCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2846) with Deadline.FourCompressed with ValueLengthUncompressed
          case object DeadlineFiveCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2847) with Deadline.FiveCompressed with ValueLengthUncompressed
          case object DeadlineSixCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2848) with Deadline.SixCompressed with ValueLengthUncompressed
          case object DeadlineSevenCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2849) with Deadline.SevenCompressed with ValueLengthUncompressed
          case object DeadlineFullyCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2850) with Deadline.FullyCompressed with ValueLengthUncompressed
          case object DeadlineUncompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2851) with Deadline.Uncompressed with ValueLengthUncompressed
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
          case object NoDeadline extends UpdateFunctionKeyPartiallyCompressedEntryId(2852) with Deadline.NoDeadline with ValueLengthOneCompressed
          case object DeadlineOneCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2853) with Deadline.OneCompressed with ValueLengthOneCompressed
          case object DeadlineTwoCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2854) with Deadline.TwoCompressed with ValueLengthOneCompressed
          case object DeadlineThreeCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2855) with Deadline.ThreeCompressed with ValueLengthOneCompressed
          case object DeadlineFourCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2856) with Deadline.FourCompressed with ValueLengthOneCompressed
          case object DeadlineFiveCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2857) with Deadline.FiveCompressed with ValueLengthOneCompressed
          case object DeadlineSixCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2858) with Deadline.SixCompressed with ValueLengthOneCompressed
          case object DeadlineSevenCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2859) with Deadline.SevenCompressed with ValueLengthOneCompressed
          case object DeadlineFullyCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2860) with Deadline.FullyCompressed with ValueLengthOneCompressed
          case object DeadlineUncompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2861) with Deadline.Uncompressed with ValueLengthOneCompressed
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
          case object NoDeadline extends UpdateFunctionKeyPartiallyCompressedEntryId(2862) with Deadline.NoDeadline with ValueLengthTwoCompressed
          case object DeadlineOneCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2863) with Deadline.OneCompressed with ValueLengthTwoCompressed
          case object DeadlineTwoCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2864) with Deadline.TwoCompressed with ValueLengthTwoCompressed
          case object DeadlineThreeCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2865) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
          case object DeadlineFourCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2866) with Deadline.FourCompressed with ValueLengthTwoCompressed
          case object DeadlineFiveCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2867) with Deadline.FiveCompressed with ValueLengthTwoCompressed
          case object DeadlineSixCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2868) with Deadline.SixCompressed with ValueLengthTwoCompressed
          case object DeadlineSevenCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2869) with Deadline.SevenCompressed with ValueLengthTwoCompressed
          case object DeadlineFullyCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2870) with Deadline.FullyCompressed with ValueLengthTwoCompressed
          case object DeadlineUncompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2871) with Deadline.Uncompressed with ValueLengthTwoCompressed
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
          case object NoDeadline extends UpdateFunctionKeyPartiallyCompressedEntryId(2872) with Deadline.NoDeadline with ValueLengthThreeCompressed
          case object DeadlineOneCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2873) with Deadline.OneCompressed with ValueLengthThreeCompressed
          case object DeadlineTwoCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2874) with Deadline.TwoCompressed with ValueLengthThreeCompressed
          case object DeadlineThreeCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2875) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
          case object DeadlineFourCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2876) with Deadline.FourCompressed with ValueLengthThreeCompressed
          case object DeadlineFiveCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2877) with Deadline.FiveCompressed with ValueLengthThreeCompressed
          case object DeadlineSixCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2878) with Deadline.SixCompressed with ValueLengthThreeCompressed
          case object DeadlineSevenCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2879) with Deadline.SevenCompressed with ValueLengthThreeCompressed
          case object DeadlineFullyCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2880) with Deadline.FullyCompressed with ValueLengthThreeCompressed
          case object DeadlineUncompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2881) with Deadline.Uncompressed with ValueLengthThreeCompressed
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
          case object NoDeadline extends UpdateFunctionKeyPartiallyCompressedEntryId(2882) with Deadline.NoDeadline with ValueLengthFullyCompressed
          case object DeadlineOneCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2883) with Deadline.OneCompressed with ValueLengthFullyCompressed
          case object DeadlineTwoCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2884) with Deadline.TwoCompressed with ValueLengthFullyCompressed
          case object DeadlineThreeCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2885) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
          case object DeadlineFourCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2886) with Deadline.FourCompressed with ValueLengthFullyCompressed
          case object DeadlineFiveCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2887) with Deadline.FiveCompressed with ValueLengthFullyCompressed
          case object DeadlineSixCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2888) with Deadline.SixCompressed with ValueLengthFullyCompressed
          case object DeadlineSevenCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2889) with Deadline.SevenCompressed with ValueLengthFullyCompressed
          case object DeadlineFullyCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2890) with Deadline.FullyCompressed with ValueLengthFullyCompressed
          case object DeadlineUncompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2891) with Deadline.Uncompressed with ValueLengthFullyCompressed
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
          case object NoDeadline extends UpdateFunctionKeyPartiallyCompressedEntryId(2892) with Deadline.NoDeadline with ValueLengthUncompressed
          case object DeadlineOneCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2893) with Deadline.OneCompressed with ValueLengthUncompressed
          case object DeadlineTwoCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2894) with Deadline.TwoCompressed with ValueLengthUncompressed
          case object DeadlineThreeCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2895) with Deadline.ThreeCompressed with ValueLengthUncompressed
          case object DeadlineFourCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2896) with Deadline.FourCompressed with ValueLengthUncompressed
          case object DeadlineFiveCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2897) with Deadline.FiveCompressed with ValueLengthUncompressed
          case object DeadlineSixCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2898) with Deadline.SixCompressed with ValueLengthUncompressed
          case object DeadlineSevenCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2899) with Deadline.SevenCompressed with ValueLengthUncompressed
          case object DeadlineFullyCompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2900) with Deadline.FullyCompressed with ValueLengthUncompressed
          case object DeadlineUncompressed extends UpdateFunctionKeyPartiallyCompressedEntryId(2901) with Deadline.Uncompressed with ValueLengthUncompressed
        }
      }
    }
  }
}