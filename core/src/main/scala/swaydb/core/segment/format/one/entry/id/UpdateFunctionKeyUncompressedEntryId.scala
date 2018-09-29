
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
sealed abstract class UpdateFunctionKeyUncompressedEntryId(override val id: Int) extends EntryId(id)
object UpdateFunctionKeyUncompressedEntryId {

  def keyIdsList: List[UpdateFunctionKeyUncompressedEntryId] = SealedList.list[UpdateFunctionKeyUncompressedEntryId].sortBy(_.id)

  private val (headId, lastId) = keyIdsList ==> {
    keyIdsList =>
      (keyIdsList.head.id, keyIdsList.last.id)
  }

  def contains(id: Int): Option[Int] =
    if (id >= headId && id <= lastId)
      Some(id)
    else
      None

  sealed trait KeyUncompressed extends Key.Uncompressed {
    override val valueFullyCompressed: Value.FullyCompressed = KeyUncompressed.ValueFullyCompressed
    override val valueUncompressed: Value.Uncompressed = KeyUncompressed.ValueUncompressed
    override val noValue: Value.NoValue = KeyUncompressed.NoValue
  }
  case object KeyUncompressed extends KeyUncompressed {

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
      case object NoDeadline extends UpdateFunctionKeyUncompressedEntryId(2903) with Deadline.NoDeadline with NoValue
      case object DeadlineOneCompressed extends UpdateFunctionKeyUncompressedEntryId(2904) with Deadline.OneCompressed with NoValue
      case object DeadlineTwoCompressed extends UpdateFunctionKeyUncompressedEntryId(2905) with Deadline.TwoCompressed with NoValue
      case object DeadlineThreeCompressed extends UpdateFunctionKeyUncompressedEntryId(2906) with Deadline.ThreeCompressed with NoValue
      case object DeadlineFourCompressed extends UpdateFunctionKeyUncompressedEntryId(2907) with Deadline.FourCompressed with NoValue
      case object DeadlineFiveCompressed extends UpdateFunctionKeyUncompressedEntryId(2908) with Deadline.FiveCompressed with NoValue
      case object DeadlineSixCompressed extends UpdateFunctionKeyUncompressedEntryId(2909) with Deadline.SixCompressed with NoValue
      case object DeadlineSevenCompressed extends UpdateFunctionKeyUncompressedEntryId(2910) with Deadline.SevenCompressed with NoValue
      case object DeadlineFullyCompressed extends UpdateFunctionKeyUncompressedEntryId(2911) with Deadline.FullyCompressed with NoValue
      case object DeadlineUncompressed extends UpdateFunctionKeyUncompressedEntryId(2912) with Deadline.Uncompressed with NoValue
    }

    sealed trait ValueFullyCompressed extends Value.FullyCompressed with KeyUncompressed {
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
      case object NoDeadline extends UpdateFunctionKeyUncompressedEntryId(2913) with Deadline.NoDeadline with ValueFullyCompressed
      case object DeadlineOneCompressed extends UpdateFunctionKeyUncompressedEntryId(2914) with Deadline.OneCompressed with ValueFullyCompressed
      case object DeadlineTwoCompressed extends UpdateFunctionKeyUncompressedEntryId(2915) with Deadline.TwoCompressed with ValueFullyCompressed
      case object DeadlineThreeCompressed extends UpdateFunctionKeyUncompressedEntryId(2916) with Deadline.ThreeCompressed with ValueFullyCompressed
      case object DeadlineFourCompressed extends UpdateFunctionKeyUncompressedEntryId(2917) with Deadline.FourCompressed with ValueFullyCompressed
      case object DeadlineFiveCompressed extends UpdateFunctionKeyUncompressedEntryId(2918) with Deadline.FiveCompressed with ValueFullyCompressed
      case object DeadlineSixCompressed extends UpdateFunctionKeyUncompressedEntryId(2919) with Deadline.SixCompressed with ValueFullyCompressed
      case object DeadlineSevenCompressed extends UpdateFunctionKeyUncompressedEntryId(2920) with Deadline.SevenCompressed with ValueFullyCompressed
      case object DeadlineFullyCompressed extends UpdateFunctionKeyUncompressedEntryId(2921) with Deadline.FullyCompressed with ValueFullyCompressed
      case object DeadlineUncompressed extends UpdateFunctionKeyUncompressedEntryId(2922) with Deadline.Uncompressed with ValueFullyCompressed
    }

    sealed trait ValueUncompressed extends Value.Uncompressed with KeyUncompressed {
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
          case object NoDeadline extends UpdateFunctionKeyUncompressedEntryId(2923) with Deadline.NoDeadline with ValueLengthOneCompressed
          case object DeadlineOneCompressed extends UpdateFunctionKeyUncompressedEntryId(2924) with Deadline.OneCompressed with ValueLengthOneCompressed
          case object DeadlineTwoCompressed extends UpdateFunctionKeyUncompressedEntryId(2925) with Deadline.TwoCompressed with ValueLengthOneCompressed
          case object DeadlineThreeCompressed extends UpdateFunctionKeyUncompressedEntryId(2926) with Deadline.ThreeCompressed with ValueLengthOneCompressed
          case object DeadlineFourCompressed extends UpdateFunctionKeyUncompressedEntryId(2927) with Deadline.FourCompressed with ValueLengthOneCompressed
          case object DeadlineFiveCompressed extends UpdateFunctionKeyUncompressedEntryId(2928) with Deadline.FiveCompressed with ValueLengthOneCompressed
          case object DeadlineSixCompressed extends UpdateFunctionKeyUncompressedEntryId(2929) with Deadline.SixCompressed with ValueLengthOneCompressed
          case object DeadlineSevenCompressed extends UpdateFunctionKeyUncompressedEntryId(2930) with Deadline.SevenCompressed with ValueLengthOneCompressed
          case object DeadlineFullyCompressed extends UpdateFunctionKeyUncompressedEntryId(2931) with Deadline.FullyCompressed with ValueLengthOneCompressed
          case object DeadlineUncompressed extends UpdateFunctionKeyUncompressedEntryId(2932) with Deadline.Uncompressed with ValueLengthOneCompressed
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
          case object NoDeadline extends UpdateFunctionKeyUncompressedEntryId(2933) with Deadline.NoDeadline with ValueLengthTwoCompressed
          case object DeadlineOneCompressed extends UpdateFunctionKeyUncompressedEntryId(2934) with Deadline.OneCompressed with ValueLengthTwoCompressed
          case object DeadlineTwoCompressed extends UpdateFunctionKeyUncompressedEntryId(2935) with Deadline.TwoCompressed with ValueLengthTwoCompressed
          case object DeadlineThreeCompressed extends UpdateFunctionKeyUncompressedEntryId(2936) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
          case object DeadlineFourCompressed extends UpdateFunctionKeyUncompressedEntryId(2937) with Deadline.FourCompressed with ValueLengthTwoCompressed
          case object DeadlineFiveCompressed extends UpdateFunctionKeyUncompressedEntryId(2938) with Deadline.FiveCompressed with ValueLengthTwoCompressed
          case object DeadlineSixCompressed extends UpdateFunctionKeyUncompressedEntryId(2939) with Deadline.SixCompressed with ValueLengthTwoCompressed
          case object DeadlineSevenCompressed extends UpdateFunctionKeyUncompressedEntryId(2940) with Deadline.SevenCompressed with ValueLengthTwoCompressed
          case object DeadlineFullyCompressed extends UpdateFunctionKeyUncompressedEntryId(2941) with Deadline.FullyCompressed with ValueLengthTwoCompressed
          case object DeadlineUncompressed extends UpdateFunctionKeyUncompressedEntryId(2942) with Deadline.Uncompressed with ValueLengthTwoCompressed
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
          case object NoDeadline extends UpdateFunctionKeyUncompressedEntryId(2943) with Deadline.NoDeadline with ValueLengthThreeCompressed
          case object DeadlineOneCompressed extends UpdateFunctionKeyUncompressedEntryId(2944) with Deadline.OneCompressed with ValueLengthThreeCompressed
          case object DeadlineTwoCompressed extends UpdateFunctionKeyUncompressedEntryId(2945) with Deadline.TwoCompressed with ValueLengthThreeCompressed
          case object DeadlineThreeCompressed extends UpdateFunctionKeyUncompressedEntryId(2946) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
          case object DeadlineFourCompressed extends UpdateFunctionKeyUncompressedEntryId(2947) with Deadline.FourCompressed with ValueLengthThreeCompressed
          case object DeadlineFiveCompressed extends UpdateFunctionKeyUncompressedEntryId(2948) with Deadline.FiveCompressed with ValueLengthThreeCompressed
          case object DeadlineSixCompressed extends UpdateFunctionKeyUncompressedEntryId(2949) with Deadline.SixCompressed with ValueLengthThreeCompressed
          case object DeadlineSevenCompressed extends UpdateFunctionKeyUncompressedEntryId(2950) with Deadline.SevenCompressed with ValueLengthThreeCompressed
          case object DeadlineFullyCompressed extends UpdateFunctionKeyUncompressedEntryId(2951) with Deadline.FullyCompressed with ValueLengthThreeCompressed
          case object DeadlineUncompressed extends UpdateFunctionKeyUncompressedEntryId(2952) with Deadline.Uncompressed with ValueLengthThreeCompressed
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
          case object NoDeadline extends UpdateFunctionKeyUncompressedEntryId(2953) with Deadline.NoDeadline with ValueLengthFullyCompressed
          case object DeadlineOneCompressed extends UpdateFunctionKeyUncompressedEntryId(2954) with Deadline.OneCompressed with ValueLengthFullyCompressed
          case object DeadlineTwoCompressed extends UpdateFunctionKeyUncompressedEntryId(2955) with Deadline.TwoCompressed with ValueLengthFullyCompressed
          case object DeadlineThreeCompressed extends UpdateFunctionKeyUncompressedEntryId(2956) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
          case object DeadlineFourCompressed extends UpdateFunctionKeyUncompressedEntryId(2957) with Deadline.FourCompressed with ValueLengthFullyCompressed
          case object DeadlineFiveCompressed extends UpdateFunctionKeyUncompressedEntryId(2958) with Deadline.FiveCompressed with ValueLengthFullyCompressed
          case object DeadlineSixCompressed extends UpdateFunctionKeyUncompressedEntryId(2959) with Deadline.SixCompressed with ValueLengthFullyCompressed
          case object DeadlineSevenCompressed extends UpdateFunctionKeyUncompressedEntryId(2960) with Deadline.SevenCompressed with ValueLengthFullyCompressed
          case object DeadlineFullyCompressed extends UpdateFunctionKeyUncompressedEntryId(2961) with Deadline.FullyCompressed with ValueLengthFullyCompressed
          case object DeadlineUncompressed extends UpdateFunctionKeyUncompressedEntryId(2962) with Deadline.Uncompressed with ValueLengthFullyCompressed
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
          case object NoDeadline extends UpdateFunctionKeyUncompressedEntryId(2963) with Deadline.NoDeadline with ValueLengthUncompressed
          case object DeadlineOneCompressed extends UpdateFunctionKeyUncompressedEntryId(2964) with Deadline.OneCompressed with ValueLengthUncompressed
          case object DeadlineTwoCompressed extends UpdateFunctionKeyUncompressedEntryId(2965) with Deadline.TwoCompressed with ValueLengthUncompressed
          case object DeadlineThreeCompressed extends UpdateFunctionKeyUncompressedEntryId(2966) with Deadline.ThreeCompressed with ValueLengthUncompressed
          case object DeadlineFourCompressed extends UpdateFunctionKeyUncompressedEntryId(2967) with Deadline.FourCompressed with ValueLengthUncompressed
          case object DeadlineFiveCompressed extends UpdateFunctionKeyUncompressedEntryId(2968) with Deadline.FiveCompressed with ValueLengthUncompressed
          case object DeadlineSixCompressed extends UpdateFunctionKeyUncompressedEntryId(2969) with Deadline.SixCompressed with ValueLengthUncompressed
          case object DeadlineSevenCompressed extends UpdateFunctionKeyUncompressedEntryId(2970) with Deadline.SevenCompressed with ValueLengthUncompressed
          case object DeadlineFullyCompressed extends UpdateFunctionKeyUncompressedEntryId(2971) with Deadline.FullyCompressed with ValueLengthUncompressed
          case object DeadlineUncompressed extends UpdateFunctionKeyUncompressedEntryId(2972) with Deadline.Uncompressed with ValueLengthUncompressed
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
          case object NoDeadline extends UpdateFunctionKeyUncompressedEntryId(2973) with Deadline.NoDeadline with ValueLengthOneCompressed
          case object DeadlineOneCompressed extends UpdateFunctionKeyUncompressedEntryId(2974) with Deadline.OneCompressed with ValueLengthOneCompressed
          case object DeadlineTwoCompressed extends UpdateFunctionKeyUncompressedEntryId(2975) with Deadline.TwoCompressed with ValueLengthOneCompressed
          case object DeadlineThreeCompressed extends UpdateFunctionKeyUncompressedEntryId(2976) with Deadline.ThreeCompressed with ValueLengthOneCompressed
          case object DeadlineFourCompressed extends UpdateFunctionKeyUncompressedEntryId(2977) with Deadline.FourCompressed with ValueLengthOneCompressed
          case object DeadlineFiveCompressed extends UpdateFunctionKeyUncompressedEntryId(2978) with Deadline.FiveCompressed with ValueLengthOneCompressed
          case object DeadlineSixCompressed extends UpdateFunctionKeyUncompressedEntryId(2979) with Deadline.SixCompressed with ValueLengthOneCompressed
          case object DeadlineSevenCompressed extends UpdateFunctionKeyUncompressedEntryId(2980) with Deadline.SevenCompressed with ValueLengthOneCompressed
          case object DeadlineFullyCompressed extends UpdateFunctionKeyUncompressedEntryId(2981) with Deadline.FullyCompressed with ValueLengthOneCompressed
          case object DeadlineUncompressed extends UpdateFunctionKeyUncompressedEntryId(2982) with Deadline.Uncompressed with ValueLengthOneCompressed
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
          case object NoDeadline extends UpdateFunctionKeyUncompressedEntryId(2983) with Deadline.NoDeadline with ValueLengthTwoCompressed
          case object DeadlineOneCompressed extends UpdateFunctionKeyUncompressedEntryId(2984) with Deadline.OneCompressed with ValueLengthTwoCompressed
          case object DeadlineTwoCompressed extends UpdateFunctionKeyUncompressedEntryId(2985) with Deadline.TwoCompressed with ValueLengthTwoCompressed
          case object DeadlineThreeCompressed extends UpdateFunctionKeyUncompressedEntryId(2986) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
          case object DeadlineFourCompressed extends UpdateFunctionKeyUncompressedEntryId(2987) with Deadline.FourCompressed with ValueLengthTwoCompressed
          case object DeadlineFiveCompressed extends UpdateFunctionKeyUncompressedEntryId(2988) with Deadline.FiveCompressed with ValueLengthTwoCompressed
          case object DeadlineSixCompressed extends UpdateFunctionKeyUncompressedEntryId(2989) with Deadline.SixCompressed with ValueLengthTwoCompressed
          case object DeadlineSevenCompressed extends UpdateFunctionKeyUncompressedEntryId(2990) with Deadline.SevenCompressed with ValueLengthTwoCompressed
          case object DeadlineFullyCompressed extends UpdateFunctionKeyUncompressedEntryId(2991) with Deadline.FullyCompressed with ValueLengthTwoCompressed
          case object DeadlineUncompressed extends UpdateFunctionKeyUncompressedEntryId(2992) with Deadline.Uncompressed with ValueLengthTwoCompressed
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
          case object NoDeadline extends UpdateFunctionKeyUncompressedEntryId(2993) with Deadline.NoDeadline with ValueLengthThreeCompressed
          case object DeadlineOneCompressed extends UpdateFunctionKeyUncompressedEntryId(2994) with Deadline.OneCompressed with ValueLengthThreeCompressed
          case object DeadlineTwoCompressed extends UpdateFunctionKeyUncompressedEntryId(2995) with Deadline.TwoCompressed with ValueLengthThreeCompressed
          case object DeadlineThreeCompressed extends UpdateFunctionKeyUncompressedEntryId(2996) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
          case object DeadlineFourCompressed extends UpdateFunctionKeyUncompressedEntryId(2997) with Deadline.FourCompressed with ValueLengthThreeCompressed
          case object DeadlineFiveCompressed extends UpdateFunctionKeyUncompressedEntryId(2998) with Deadline.FiveCompressed with ValueLengthThreeCompressed
          case object DeadlineSixCompressed extends UpdateFunctionKeyUncompressedEntryId(2999) with Deadline.SixCompressed with ValueLengthThreeCompressed
          case object DeadlineSevenCompressed extends UpdateFunctionKeyUncompressedEntryId(3000) with Deadline.SevenCompressed with ValueLengthThreeCompressed
          case object DeadlineFullyCompressed extends UpdateFunctionKeyUncompressedEntryId(3001) with Deadline.FullyCompressed with ValueLengthThreeCompressed
          case object DeadlineUncompressed extends UpdateFunctionKeyUncompressedEntryId(3002) with Deadline.Uncompressed with ValueLengthThreeCompressed
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
          case object NoDeadline extends UpdateFunctionKeyUncompressedEntryId(3003) with Deadline.NoDeadline with ValueLengthFullyCompressed
          case object DeadlineOneCompressed extends UpdateFunctionKeyUncompressedEntryId(3004) with Deadline.OneCompressed with ValueLengthFullyCompressed
          case object DeadlineTwoCompressed extends UpdateFunctionKeyUncompressedEntryId(3005) with Deadline.TwoCompressed with ValueLengthFullyCompressed
          case object DeadlineThreeCompressed extends UpdateFunctionKeyUncompressedEntryId(3006) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
          case object DeadlineFourCompressed extends UpdateFunctionKeyUncompressedEntryId(3007) with Deadline.FourCompressed with ValueLengthFullyCompressed
          case object DeadlineFiveCompressed extends UpdateFunctionKeyUncompressedEntryId(3008) with Deadline.FiveCompressed with ValueLengthFullyCompressed
          case object DeadlineSixCompressed extends UpdateFunctionKeyUncompressedEntryId(3009) with Deadline.SixCompressed with ValueLengthFullyCompressed
          case object DeadlineSevenCompressed extends UpdateFunctionKeyUncompressedEntryId(3010) with Deadline.SevenCompressed with ValueLengthFullyCompressed
          case object DeadlineFullyCompressed extends UpdateFunctionKeyUncompressedEntryId(3011) with Deadline.FullyCompressed with ValueLengthFullyCompressed
          case object DeadlineUncompressed extends UpdateFunctionKeyUncompressedEntryId(3012) with Deadline.Uncompressed with ValueLengthFullyCompressed
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
          case object NoDeadline extends UpdateFunctionKeyUncompressedEntryId(3013) with Deadline.NoDeadline with ValueLengthUncompressed
          case object DeadlineOneCompressed extends UpdateFunctionKeyUncompressedEntryId(3014) with Deadline.OneCompressed with ValueLengthUncompressed
          case object DeadlineTwoCompressed extends UpdateFunctionKeyUncompressedEntryId(3015) with Deadline.TwoCompressed with ValueLengthUncompressed
          case object DeadlineThreeCompressed extends UpdateFunctionKeyUncompressedEntryId(3016) with Deadline.ThreeCompressed with ValueLengthUncompressed
          case object DeadlineFourCompressed extends UpdateFunctionKeyUncompressedEntryId(3017) with Deadline.FourCompressed with ValueLengthUncompressed
          case object DeadlineFiveCompressed extends UpdateFunctionKeyUncompressedEntryId(3018) with Deadline.FiveCompressed with ValueLengthUncompressed
          case object DeadlineSixCompressed extends UpdateFunctionKeyUncompressedEntryId(3019) with Deadline.SixCompressed with ValueLengthUncompressed
          case object DeadlineSevenCompressed extends UpdateFunctionKeyUncompressedEntryId(3020) with Deadline.SevenCompressed with ValueLengthUncompressed
          case object DeadlineFullyCompressed extends UpdateFunctionKeyUncompressedEntryId(3021) with Deadline.FullyCompressed with ValueLengthUncompressed
          case object DeadlineUncompressed extends UpdateFunctionKeyUncompressedEntryId(3022) with Deadline.Uncompressed with ValueLengthUncompressed
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
          case object NoDeadline extends UpdateFunctionKeyUncompressedEntryId(3023) with Deadline.NoDeadline with ValueLengthOneCompressed
          case object DeadlineOneCompressed extends UpdateFunctionKeyUncompressedEntryId(3024) with Deadline.OneCompressed with ValueLengthOneCompressed
          case object DeadlineTwoCompressed extends UpdateFunctionKeyUncompressedEntryId(3025) with Deadline.TwoCompressed with ValueLengthOneCompressed
          case object DeadlineThreeCompressed extends UpdateFunctionKeyUncompressedEntryId(3026) with Deadline.ThreeCompressed with ValueLengthOneCompressed
          case object DeadlineFourCompressed extends UpdateFunctionKeyUncompressedEntryId(3027) with Deadline.FourCompressed with ValueLengthOneCompressed
          case object DeadlineFiveCompressed extends UpdateFunctionKeyUncompressedEntryId(3028) with Deadline.FiveCompressed with ValueLengthOneCompressed
          case object DeadlineSixCompressed extends UpdateFunctionKeyUncompressedEntryId(3029) with Deadline.SixCompressed with ValueLengthOneCompressed
          case object DeadlineSevenCompressed extends UpdateFunctionKeyUncompressedEntryId(3030) with Deadline.SevenCompressed with ValueLengthOneCompressed
          case object DeadlineFullyCompressed extends UpdateFunctionKeyUncompressedEntryId(3031) with Deadline.FullyCompressed with ValueLengthOneCompressed
          case object DeadlineUncompressed extends UpdateFunctionKeyUncompressedEntryId(3032) with Deadline.Uncompressed with ValueLengthOneCompressed
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
          case object NoDeadline extends UpdateFunctionKeyUncompressedEntryId(3033) with Deadline.NoDeadline with ValueLengthTwoCompressed
          case object DeadlineOneCompressed extends UpdateFunctionKeyUncompressedEntryId(3034) with Deadline.OneCompressed with ValueLengthTwoCompressed
          case object DeadlineTwoCompressed extends UpdateFunctionKeyUncompressedEntryId(3035) with Deadline.TwoCompressed with ValueLengthTwoCompressed
          case object DeadlineThreeCompressed extends UpdateFunctionKeyUncompressedEntryId(3036) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
          case object DeadlineFourCompressed extends UpdateFunctionKeyUncompressedEntryId(3037) with Deadline.FourCompressed with ValueLengthTwoCompressed
          case object DeadlineFiveCompressed extends UpdateFunctionKeyUncompressedEntryId(3038) with Deadline.FiveCompressed with ValueLengthTwoCompressed
          case object DeadlineSixCompressed extends UpdateFunctionKeyUncompressedEntryId(3039) with Deadline.SixCompressed with ValueLengthTwoCompressed
          case object DeadlineSevenCompressed extends UpdateFunctionKeyUncompressedEntryId(3040) with Deadline.SevenCompressed with ValueLengthTwoCompressed
          case object DeadlineFullyCompressed extends UpdateFunctionKeyUncompressedEntryId(3041) with Deadline.FullyCompressed with ValueLengthTwoCompressed
          case object DeadlineUncompressed extends UpdateFunctionKeyUncompressedEntryId(3042) with Deadline.Uncompressed with ValueLengthTwoCompressed
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
          case object NoDeadline extends UpdateFunctionKeyUncompressedEntryId(3043) with Deadline.NoDeadline with ValueLengthThreeCompressed
          case object DeadlineOneCompressed extends UpdateFunctionKeyUncompressedEntryId(3044) with Deadline.OneCompressed with ValueLengthThreeCompressed
          case object DeadlineTwoCompressed extends UpdateFunctionKeyUncompressedEntryId(3045) with Deadline.TwoCompressed with ValueLengthThreeCompressed
          case object DeadlineThreeCompressed extends UpdateFunctionKeyUncompressedEntryId(3046) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
          case object DeadlineFourCompressed extends UpdateFunctionKeyUncompressedEntryId(3047) with Deadline.FourCompressed with ValueLengthThreeCompressed
          case object DeadlineFiveCompressed extends UpdateFunctionKeyUncompressedEntryId(3048) with Deadline.FiveCompressed with ValueLengthThreeCompressed
          case object DeadlineSixCompressed extends UpdateFunctionKeyUncompressedEntryId(3049) with Deadline.SixCompressed with ValueLengthThreeCompressed
          case object DeadlineSevenCompressed extends UpdateFunctionKeyUncompressedEntryId(3050) with Deadline.SevenCompressed with ValueLengthThreeCompressed
          case object DeadlineFullyCompressed extends UpdateFunctionKeyUncompressedEntryId(3051) with Deadline.FullyCompressed with ValueLengthThreeCompressed
          case object DeadlineUncompressed extends UpdateFunctionKeyUncompressedEntryId(3052) with Deadline.Uncompressed with ValueLengthThreeCompressed
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
          case object NoDeadline extends UpdateFunctionKeyUncompressedEntryId(3053) with Deadline.NoDeadline with ValueLengthFullyCompressed
          case object DeadlineOneCompressed extends UpdateFunctionKeyUncompressedEntryId(3054) with Deadline.OneCompressed with ValueLengthFullyCompressed
          case object DeadlineTwoCompressed extends UpdateFunctionKeyUncompressedEntryId(3055) with Deadline.TwoCompressed with ValueLengthFullyCompressed
          case object DeadlineThreeCompressed extends UpdateFunctionKeyUncompressedEntryId(3056) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
          case object DeadlineFourCompressed extends UpdateFunctionKeyUncompressedEntryId(3057) with Deadline.FourCompressed with ValueLengthFullyCompressed
          case object DeadlineFiveCompressed extends UpdateFunctionKeyUncompressedEntryId(3058) with Deadline.FiveCompressed with ValueLengthFullyCompressed
          case object DeadlineSixCompressed extends UpdateFunctionKeyUncompressedEntryId(3059) with Deadline.SixCompressed with ValueLengthFullyCompressed
          case object DeadlineSevenCompressed extends UpdateFunctionKeyUncompressedEntryId(3060) with Deadline.SevenCompressed with ValueLengthFullyCompressed
          case object DeadlineFullyCompressed extends UpdateFunctionKeyUncompressedEntryId(3061) with Deadline.FullyCompressed with ValueLengthFullyCompressed
          case object DeadlineUncompressed extends UpdateFunctionKeyUncompressedEntryId(3062) with Deadline.Uncompressed with ValueLengthFullyCompressed
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
          case object NoDeadline extends UpdateFunctionKeyUncompressedEntryId(3063) with Deadline.NoDeadline with ValueLengthUncompressed
          case object DeadlineOneCompressed extends UpdateFunctionKeyUncompressedEntryId(3064) with Deadline.OneCompressed with ValueLengthUncompressed
          case object DeadlineTwoCompressed extends UpdateFunctionKeyUncompressedEntryId(3065) with Deadline.TwoCompressed with ValueLengthUncompressed
          case object DeadlineThreeCompressed extends UpdateFunctionKeyUncompressedEntryId(3066) with Deadline.ThreeCompressed with ValueLengthUncompressed
          case object DeadlineFourCompressed extends UpdateFunctionKeyUncompressedEntryId(3067) with Deadline.FourCompressed with ValueLengthUncompressed
          case object DeadlineFiveCompressed extends UpdateFunctionKeyUncompressedEntryId(3068) with Deadline.FiveCompressed with ValueLengthUncompressed
          case object DeadlineSixCompressed extends UpdateFunctionKeyUncompressedEntryId(3069) with Deadline.SixCompressed with ValueLengthUncompressed
          case object DeadlineSevenCompressed extends UpdateFunctionKeyUncompressedEntryId(3070) with Deadline.SevenCompressed with ValueLengthUncompressed
          case object DeadlineFullyCompressed extends UpdateFunctionKeyUncompressedEntryId(3071) with Deadline.FullyCompressed with ValueLengthUncompressed
          case object DeadlineUncompressed extends UpdateFunctionKeyUncompressedEntryId(3072) with Deadline.Uncompressed with ValueLengthUncompressed
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
          case object NoDeadline extends UpdateFunctionKeyUncompressedEntryId(3073) with Deadline.NoDeadline with ValueLengthOneCompressed
          case object DeadlineOneCompressed extends UpdateFunctionKeyUncompressedEntryId(3074) with Deadline.OneCompressed with ValueLengthOneCompressed
          case object DeadlineTwoCompressed extends UpdateFunctionKeyUncompressedEntryId(3075) with Deadline.TwoCompressed with ValueLengthOneCompressed
          case object DeadlineThreeCompressed extends UpdateFunctionKeyUncompressedEntryId(3076) with Deadline.ThreeCompressed with ValueLengthOneCompressed
          case object DeadlineFourCompressed extends UpdateFunctionKeyUncompressedEntryId(3077) with Deadline.FourCompressed with ValueLengthOneCompressed
          case object DeadlineFiveCompressed extends UpdateFunctionKeyUncompressedEntryId(3078) with Deadline.FiveCompressed with ValueLengthOneCompressed
          case object DeadlineSixCompressed extends UpdateFunctionKeyUncompressedEntryId(3079) with Deadline.SixCompressed with ValueLengthOneCompressed
          case object DeadlineSevenCompressed extends UpdateFunctionKeyUncompressedEntryId(3080) with Deadline.SevenCompressed with ValueLengthOneCompressed
          case object DeadlineFullyCompressed extends UpdateFunctionKeyUncompressedEntryId(3081) with Deadline.FullyCompressed with ValueLengthOneCompressed
          case object DeadlineUncompressed extends UpdateFunctionKeyUncompressedEntryId(3082) with Deadline.Uncompressed with ValueLengthOneCompressed
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
          case object NoDeadline extends UpdateFunctionKeyUncompressedEntryId(3083) with Deadline.NoDeadline with ValueLengthTwoCompressed
          case object DeadlineOneCompressed extends UpdateFunctionKeyUncompressedEntryId(3084) with Deadline.OneCompressed with ValueLengthTwoCompressed
          case object DeadlineTwoCompressed extends UpdateFunctionKeyUncompressedEntryId(3085) with Deadline.TwoCompressed with ValueLengthTwoCompressed
          case object DeadlineThreeCompressed extends UpdateFunctionKeyUncompressedEntryId(3086) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
          case object DeadlineFourCompressed extends UpdateFunctionKeyUncompressedEntryId(3087) with Deadline.FourCompressed with ValueLengthTwoCompressed
          case object DeadlineFiveCompressed extends UpdateFunctionKeyUncompressedEntryId(3088) with Deadline.FiveCompressed with ValueLengthTwoCompressed
          case object DeadlineSixCompressed extends UpdateFunctionKeyUncompressedEntryId(3089) with Deadline.SixCompressed with ValueLengthTwoCompressed
          case object DeadlineSevenCompressed extends UpdateFunctionKeyUncompressedEntryId(3090) with Deadline.SevenCompressed with ValueLengthTwoCompressed
          case object DeadlineFullyCompressed extends UpdateFunctionKeyUncompressedEntryId(3091) with Deadline.FullyCompressed with ValueLengthTwoCompressed
          case object DeadlineUncompressed extends UpdateFunctionKeyUncompressedEntryId(3092) with Deadline.Uncompressed with ValueLengthTwoCompressed
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
          case object NoDeadline extends UpdateFunctionKeyUncompressedEntryId(3093) with Deadline.NoDeadline with ValueLengthThreeCompressed
          case object DeadlineOneCompressed extends UpdateFunctionKeyUncompressedEntryId(3094) with Deadline.OneCompressed with ValueLengthThreeCompressed
          case object DeadlineTwoCompressed extends UpdateFunctionKeyUncompressedEntryId(3095) with Deadline.TwoCompressed with ValueLengthThreeCompressed
          case object DeadlineThreeCompressed extends UpdateFunctionKeyUncompressedEntryId(3096) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
          case object DeadlineFourCompressed extends UpdateFunctionKeyUncompressedEntryId(3097) with Deadline.FourCompressed with ValueLengthThreeCompressed
          case object DeadlineFiveCompressed extends UpdateFunctionKeyUncompressedEntryId(3098) with Deadline.FiveCompressed with ValueLengthThreeCompressed
          case object DeadlineSixCompressed extends UpdateFunctionKeyUncompressedEntryId(3099) with Deadline.SixCompressed with ValueLengthThreeCompressed
          case object DeadlineSevenCompressed extends UpdateFunctionKeyUncompressedEntryId(3100) with Deadline.SevenCompressed with ValueLengthThreeCompressed
          case object DeadlineFullyCompressed extends UpdateFunctionKeyUncompressedEntryId(3101) with Deadline.FullyCompressed with ValueLengthThreeCompressed
          case object DeadlineUncompressed extends UpdateFunctionKeyUncompressedEntryId(3102) with Deadline.Uncompressed with ValueLengthThreeCompressed
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
          case object NoDeadline extends UpdateFunctionKeyUncompressedEntryId(3103) with Deadline.NoDeadline with ValueLengthFullyCompressed
          case object DeadlineOneCompressed extends UpdateFunctionKeyUncompressedEntryId(3104) with Deadline.OneCompressed with ValueLengthFullyCompressed
          case object DeadlineTwoCompressed extends UpdateFunctionKeyUncompressedEntryId(3105) with Deadline.TwoCompressed with ValueLengthFullyCompressed
          case object DeadlineThreeCompressed extends UpdateFunctionKeyUncompressedEntryId(3106) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
          case object DeadlineFourCompressed extends UpdateFunctionKeyUncompressedEntryId(3107) with Deadline.FourCompressed with ValueLengthFullyCompressed
          case object DeadlineFiveCompressed extends UpdateFunctionKeyUncompressedEntryId(3108) with Deadline.FiveCompressed with ValueLengthFullyCompressed
          case object DeadlineSixCompressed extends UpdateFunctionKeyUncompressedEntryId(3109) with Deadline.SixCompressed with ValueLengthFullyCompressed
          case object DeadlineSevenCompressed extends UpdateFunctionKeyUncompressedEntryId(3110) with Deadline.SevenCompressed with ValueLengthFullyCompressed
          case object DeadlineFullyCompressed extends UpdateFunctionKeyUncompressedEntryId(3111) with Deadline.FullyCompressed with ValueLengthFullyCompressed
          case object DeadlineUncompressed extends UpdateFunctionKeyUncompressedEntryId(3112) with Deadline.Uncompressed with ValueLengthFullyCompressed
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
          case object NoDeadline extends UpdateFunctionKeyUncompressedEntryId(3113) with Deadline.NoDeadline with ValueLengthUncompressed
          case object DeadlineOneCompressed extends UpdateFunctionKeyUncompressedEntryId(3114) with Deadline.OneCompressed with ValueLengthUncompressed
          case object DeadlineTwoCompressed extends UpdateFunctionKeyUncompressedEntryId(3115) with Deadline.TwoCompressed with ValueLengthUncompressed
          case object DeadlineThreeCompressed extends UpdateFunctionKeyUncompressedEntryId(3116) with Deadline.ThreeCompressed with ValueLengthUncompressed
          case object DeadlineFourCompressed extends UpdateFunctionKeyUncompressedEntryId(3117) with Deadline.FourCompressed with ValueLengthUncompressed
          case object DeadlineFiveCompressed extends UpdateFunctionKeyUncompressedEntryId(3118) with Deadline.FiveCompressed with ValueLengthUncompressed
          case object DeadlineSixCompressed extends UpdateFunctionKeyUncompressedEntryId(3119) with Deadline.SixCompressed with ValueLengthUncompressed
          case object DeadlineSevenCompressed extends UpdateFunctionKeyUncompressedEntryId(3120) with Deadline.SevenCompressed with ValueLengthUncompressed
          case object DeadlineFullyCompressed extends UpdateFunctionKeyUncompressedEntryId(3121) with Deadline.FullyCompressed with ValueLengthUncompressed
          case object DeadlineUncompressed extends UpdateFunctionKeyUncompressedEntryId(3122) with Deadline.Uncompressed with ValueLengthUncompressed
        }
      }
    }
  }
}