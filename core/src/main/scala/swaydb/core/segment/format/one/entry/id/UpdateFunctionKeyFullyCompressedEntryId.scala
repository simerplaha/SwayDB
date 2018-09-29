
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
sealed abstract class UpdateFunctionKeyFullyCompressedEntryId(override val id: Int) extends EntryId(id)
object UpdateFunctionKeyFullyCompressedEntryId {

  def keyIdsList: List[UpdateFunctionKeyFullyCompressedEntryId] = SealedList.list[UpdateFunctionKeyFullyCompressedEntryId].sortBy(_.id)

  private val (headId, lastId) = keyIdsList ==> {
    keyIdsList =>
      (keyIdsList.head.id, keyIdsList.last.id)
  }

  def contains(id: Int): Option[Int] =
    if (id >= headId && id <= lastId)
      Some(id)
    else
      None

  sealed trait KeyFullyCompressed extends Key.FullyCompressed {
    override val valueFullyCompressed: Value.FullyCompressed = KeyFullyCompressed.ValueFullyCompressed
    override val valueUncompressed: Value.Uncompressed = KeyFullyCompressed.ValueUncompressed
    override val noValue: Value.NoValue = KeyFullyCompressed.NoValue
  }
  case object KeyFullyCompressed extends KeyFullyCompressed {

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
      case object NoDeadline extends UpdateFunctionKeyFullyCompressedEntryId(3124) with Deadline.NoDeadline with NoValue
      case object DeadlineOneCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3125) with Deadline.OneCompressed with NoValue
      case object DeadlineTwoCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3126) with Deadline.TwoCompressed with NoValue
      case object DeadlineThreeCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3127) with Deadline.ThreeCompressed with NoValue
      case object DeadlineFourCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3128) with Deadline.FourCompressed with NoValue
      case object DeadlineFiveCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3129) with Deadline.FiveCompressed with NoValue
      case object DeadlineSixCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3130) with Deadline.SixCompressed with NoValue
      case object DeadlineSevenCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3131) with Deadline.SevenCompressed with NoValue
      case object DeadlineFullyCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3132) with Deadline.FullyCompressed with NoValue
      case object DeadlineUncompressed extends UpdateFunctionKeyFullyCompressedEntryId(3133) with Deadline.Uncompressed with NoValue
    }

    sealed trait ValueFullyCompressed extends Value.FullyCompressed with KeyFullyCompressed {
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
      case object NoDeadline extends UpdateFunctionKeyFullyCompressedEntryId(3134) with Deadline.NoDeadline with ValueFullyCompressed
      case object DeadlineOneCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3135) with Deadline.OneCompressed with ValueFullyCompressed
      case object DeadlineTwoCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3136) with Deadline.TwoCompressed with ValueFullyCompressed
      case object DeadlineThreeCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3137) with Deadline.ThreeCompressed with ValueFullyCompressed
      case object DeadlineFourCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3138) with Deadline.FourCompressed with ValueFullyCompressed
      case object DeadlineFiveCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3139) with Deadline.FiveCompressed with ValueFullyCompressed
      case object DeadlineSixCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3140) with Deadline.SixCompressed with ValueFullyCompressed
      case object DeadlineSevenCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3141) with Deadline.SevenCompressed with ValueFullyCompressed
      case object DeadlineFullyCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3142) with Deadline.FullyCompressed with ValueFullyCompressed
      case object DeadlineUncompressed extends UpdateFunctionKeyFullyCompressedEntryId(3143) with Deadline.Uncompressed with ValueFullyCompressed
    }

    sealed trait ValueUncompressed extends Value.Uncompressed with KeyFullyCompressed {
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
          case object NoDeadline extends UpdateFunctionKeyFullyCompressedEntryId(3144) with Deadline.NoDeadline with ValueLengthOneCompressed
          case object DeadlineOneCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3145) with Deadline.OneCompressed with ValueLengthOneCompressed
          case object DeadlineTwoCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3146) with Deadline.TwoCompressed with ValueLengthOneCompressed
          case object DeadlineThreeCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3147) with Deadline.ThreeCompressed with ValueLengthOneCompressed
          case object DeadlineFourCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3148) with Deadline.FourCompressed with ValueLengthOneCompressed
          case object DeadlineFiveCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3149) with Deadline.FiveCompressed with ValueLengthOneCompressed
          case object DeadlineSixCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3150) with Deadline.SixCompressed with ValueLengthOneCompressed
          case object DeadlineSevenCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3151) with Deadline.SevenCompressed with ValueLengthOneCompressed
          case object DeadlineFullyCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3152) with Deadline.FullyCompressed with ValueLengthOneCompressed
          case object DeadlineUncompressed extends UpdateFunctionKeyFullyCompressedEntryId(3153) with Deadline.Uncompressed with ValueLengthOneCompressed
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
          case object NoDeadline extends UpdateFunctionKeyFullyCompressedEntryId(3154) with Deadline.NoDeadline with ValueLengthTwoCompressed
          case object DeadlineOneCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3155) with Deadline.OneCompressed with ValueLengthTwoCompressed
          case object DeadlineTwoCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3156) with Deadline.TwoCompressed with ValueLengthTwoCompressed
          case object DeadlineThreeCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3157) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
          case object DeadlineFourCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3158) with Deadline.FourCompressed with ValueLengthTwoCompressed
          case object DeadlineFiveCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3159) with Deadline.FiveCompressed with ValueLengthTwoCompressed
          case object DeadlineSixCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3160) with Deadline.SixCompressed with ValueLengthTwoCompressed
          case object DeadlineSevenCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3161) with Deadline.SevenCompressed with ValueLengthTwoCompressed
          case object DeadlineFullyCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3162) with Deadline.FullyCompressed with ValueLengthTwoCompressed
          case object DeadlineUncompressed extends UpdateFunctionKeyFullyCompressedEntryId(3163) with Deadline.Uncompressed with ValueLengthTwoCompressed
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
          case object NoDeadline extends UpdateFunctionKeyFullyCompressedEntryId(3164) with Deadline.NoDeadline with ValueLengthThreeCompressed
          case object DeadlineOneCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3165) with Deadline.OneCompressed with ValueLengthThreeCompressed
          case object DeadlineTwoCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3166) with Deadline.TwoCompressed with ValueLengthThreeCompressed
          case object DeadlineThreeCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3167) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
          case object DeadlineFourCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3168) with Deadline.FourCompressed with ValueLengthThreeCompressed
          case object DeadlineFiveCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3169) with Deadline.FiveCompressed with ValueLengthThreeCompressed
          case object DeadlineSixCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3170) with Deadline.SixCompressed with ValueLengthThreeCompressed
          case object DeadlineSevenCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3171) with Deadline.SevenCompressed with ValueLengthThreeCompressed
          case object DeadlineFullyCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3172) with Deadline.FullyCompressed with ValueLengthThreeCompressed
          case object DeadlineUncompressed extends UpdateFunctionKeyFullyCompressedEntryId(3173) with Deadline.Uncompressed with ValueLengthThreeCompressed
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
          case object NoDeadline extends UpdateFunctionKeyFullyCompressedEntryId(3174) with Deadline.NoDeadline with ValueLengthFullyCompressed
          case object DeadlineOneCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3175) with Deadline.OneCompressed with ValueLengthFullyCompressed
          case object DeadlineTwoCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3176) with Deadline.TwoCompressed with ValueLengthFullyCompressed
          case object DeadlineThreeCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3177) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
          case object DeadlineFourCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3178) with Deadline.FourCompressed with ValueLengthFullyCompressed
          case object DeadlineFiveCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3179) with Deadline.FiveCompressed with ValueLengthFullyCompressed
          case object DeadlineSixCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3180) with Deadline.SixCompressed with ValueLengthFullyCompressed
          case object DeadlineSevenCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3181) with Deadline.SevenCompressed with ValueLengthFullyCompressed
          case object DeadlineFullyCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3182) with Deadline.FullyCompressed with ValueLengthFullyCompressed
          case object DeadlineUncompressed extends UpdateFunctionKeyFullyCompressedEntryId(3183) with Deadline.Uncompressed with ValueLengthFullyCompressed
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
          case object NoDeadline extends UpdateFunctionKeyFullyCompressedEntryId(3184) with Deadline.NoDeadline with ValueLengthUncompressed
          case object DeadlineOneCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3185) with Deadline.OneCompressed with ValueLengthUncompressed
          case object DeadlineTwoCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3186) with Deadline.TwoCompressed with ValueLengthUncompressed
          case object DeadlineThreeCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3187) with Deadline.ThreeCompressed with ValueLengthUncompressed
          case object DeadlineFourCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3188) with Deadline.FourCompressed with ValueLengthUncompressed
          case object DeadlineFiveCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3189) with Deadline.FiveCompressed with ValueLengthUncompressed
          case object DeadlineSixCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3190) with Deadline.SixCompressed with ValueLengthUncompressed
          case object DeadlineSevenCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3191) with Deadline.SevenCompressed with ValueLengthUncompressed
          case object DeadlineFullyCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3192) with Deadline.FullyCompressed with ValueLengthUncompressed
          case object DeadlineUncompressed extends UpdateFunctionKeyFullyCompressedEntryId(3193) with Deadline.Uncompressed with ValueLengthUncompressed
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
          case object NoDeadline extends UpdateFunctionKeyFullyCompressedEntryId(3194) with Deadline.NoDeadline with ValueLengthOneCompressed
          case object DeadlineOneCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3195) with Deadline.OneCompressed with ValueLengthOneCompressed
          case object DeadlineTwoCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3196) with Deadline.TwoCompressed with ValueLengthOneCompressed
          case object DeadlineThreeCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3197) with Deadline.ThreeCompressed with ValueLengthOneCompressed
          case object DeadlineFourCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3198) with Deadline.FourCompressed with ValueLengthOneCompressed
          case object DeadlineFiveCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3199) with Deadline.FiveCompressed with ValueLengthOneCompressed
          case object DeadlineSixCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3200) with Deadline.SixCompressed with ValueLengthOneCompressed
          case object DeadlineSevenCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3201) with Deadline.SevenCompressed with ValueLengthOneCompressed
          case object DeadlineFullyCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3202) with Deadline.FullyCompressed with ValueLengthOneCompressed
          case object DeadlineUncompressed extends UpdateFunctionKeyFullyCompressedEntryId(3203) with Deadline.Uncompressed with ValueLengthOneCompressed
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
          case object NoDeadline extends UpdateFunctionKeyFullyCompressedEntryId(3204) with Deadline.NoDeadline with ValueLengthTwoCompressed
          case object DeadlineOneCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3205) with Deadline.OneCompressed with ValueLengthTwoCompressed
          case object DeadlineTwoCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3206) with Deadline.TwoCompressed with ValueLengthTwoCompressed
          case object DeadlineThreeCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3207) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
          case object DeadlineFourCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3208) with Deadline.FourCompressed with ValueLengthTwoCompressed
          case object DeadlineFiveCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3209) with Deadline.FiveCompressed with ValueLengthTwoCompressed
          case object DeadlineSixCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3210) with Deadline.SixCompressed with ValueLengthTwoCompressed
          case object DeadlineSevenCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3211) with Deadline.SevenCompressed with ValueLengthTwoCompressed
          case object DeadlineFullyCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3212) with Deadline.FullyCompressed with ValueLengthTwoCompressed
          case object DeadlineUncompressed extends UpdateFunctionKeyFullyCompressedEntryId(3213) with Deadline.Uncompressed with ValueLengthTwoCompressed
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
          case object NoDeadline extends UpdateFunctionKeyFullyCompressedEntryId(3214) with Deadline.NoDeadline with ValueLengthThreeCompressed
          case object DeadlineOneCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3215) with Deadline.OneCompressed with ValueLengthThreeCompressed
          case object DeadlineTwoCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3216) with Deadline.TwoCompressed with ValueLengthThreeCompressed
          case object DeadlineThreeCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3217) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
          case object DeadlineFourCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3218) with Deadline.FourCompressed with ValueLengthThreeCompressed
          case object DeadlineFiveCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3219) with Deadline.FiveCompressed with ValueLengthThreeCompressed
          case object DeadlineSixCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3220) with Deadline.SixCompressed with ValueLengthThreeCompressed
          case object DeadlineSevenCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3221) with Deadline.SevenCompressed with ValueLengthThreeCompressed
          case object DeadlineFullyCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3222) with Deadline.FullyCompressed with ValueLengthThreeCompressed
          case object DeadlineUncompressed extends UpdateFunctionKeyFullyCompressedEntryId(3223) with Deadline.Uncompressed with ValueLengthThreeCompressed
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
          case object NoDeadline extends UpdateFunctionKeyFullyCompressedEntryId(3224) with Deadline.NoDeadline with ValueLengthFullyCompressed
          case object DeadlineOneCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3225) with Deadline.OneCompressed with ValueLengthFullyCompressed
          case object DeadlineTwoCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3226) with Deadline.TwoCompressed with ValueLengthFullyCompressed
          case object DeadlineThreeCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3227) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
          case object DeadlineFourCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3228) with Deadline.FourCompressed with ValueLengthFullyCompressed
          case object DeadlineFiveCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3229) with Deadline.FiveCompressed with ValueLengthFullyCompressed
          case object DeadlineSixCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3230) with Deadline.SixCompressed with ValueLengthFullyCompressed
          case object DeadlineSevenCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3231) with Deadline.SevenCompressed with ValueLengthFullyCompressed
          case object DeadlineFullyCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3232) with Deadline.FullyCompressed with ValueLengthFullyCompressed
          case object DeadlineUncompressed extends UpdateFunctionKeyFullyCompressedEntryId(3233) with Deadline.Uncompressed with ValueLengthFullyCompressed
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
          case object NoDeadline extends UpdateFunctionKeyFullyCompressedEntryId(3234) with Deadline.NoDeadline with ValueLengthUncompressed
          case object DeadlineOneCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3235) with Deadline.OneCompressed with ValueLengthUncompressed
          case object DeadlineTwoCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3236) with Deadline.TwoCompressed with ValueLengthUncompressed
          case object DeadlineThreeCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3237) with Deadline.ThreeCompressed with ValueLengthUncompressed
          case object DeadlineFourCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3238) with Deadline.FourCompressed with ValueLengthUncompressed
          case object DeadlineFiveCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3239) with Deadline.FiveCompressed with ValueLengthUncompressed
          case object DeadlineSixCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3240) with Deadline.SixCompressed with ValueLengthUncompressed
          case object DeadlineSevenCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3241) with Deadline.SevenCompressed with ValueLengthUncompressed
          case object DeadlineFullyCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3242) with Deadline.FullyCompressed with ValueLengthUncompressed
          case object DeadlineUncompressed extends UpdateFunctionKeyFullyCompressedEntryId(3243) with Deadline.Uncompressed with ValueLengthUncompressed
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
          case object NoDeadline extends UpdateFunctionKeyFullyCompressedEntryId(3244) with Deadline.NoDeadline with ValueLengthOneCompressed
          case object DeadlineOneCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3245) with Deadline.OneCompressed with ValueLengthOneCompressed
          case object DeadlineTwoCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3246) with Deadline.TwoCompressed with ValueLengthOneCompressed
          case object DeadlineThreeCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3247) with Deadline.ThreeCompressed with ValueLengthOneCompressed
          case object DeadlineFourCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3248) with Deadline.FourCompressed with ValueLengthOneCompressed
          case object DeadlineFiveCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3249) with Deadline.FiveCompressed with ValueLengthOneCompressed
          case object DeadlineSixCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3250) with Deadline.SixCompressed with ValueLengthOneCompressed
          case object DeadlineSevenCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3251) with Deadline.SevenCompressed with ValueLengthOneCompressed
          case object DeadlineFullyCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3252) with Deadline.FullyCompressed with ValueLengthOneCompressed
          case object DeadlineUncompressed extends UpdateFunctionKeyFullyCompressedEntryId(3253) with Deadline.Uncompressed with ValueLengthOneCompressed
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
          case object NoDeadline extends UpdateFunctionKeyFullyCompressedEntryId(3254) with Deadline.NoDeadline with ValueLengthTwoCompressed
          case object DeadlineOneCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3255) with Deadline.OneCompressed with ValueLengthTwoCompressed
          case object DeadlineTwoCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3256) with Deadline.TwoCompressed with ValueLengthTwoCompressed
          case object DeadlineThreeCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3257) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
          case object DeadlineFourCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3258) with Deadline.FourCompressed with ValueLengthTwoCompressed
          case object DeadlineFiveCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3259) with Deadline.FiveCompressed with ValueLengthTwoCompressed
          case object DeadlineSixCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3260) with Deadline.SixCompressed with ValueLengthTwoCompressed
          case object DeadlineSevenCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3261) with Deadline.SevenCompressed with ValueLengthTwoCompressed
          case object DeadlineFullyCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3262) with Deadline.FullyCompressed with ValueLengthTwoCompressed
          case object DeadlineUncompressed extends UpdateFunctionKeyFullyCompressedEntryId(3263) with Deadline.Uncompressed with ValueLengthTwoCompressed
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
          case object NoDeadline extends UpdateFunctionKeyFullyCompressedEntryId(3264) with Deadline.NoDeadline with ValueLengthThreeCompressed
          case object DeadlineOneCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3265) with Deadline.OneCompressed with ValueLengthThreeCompressed
          case object DeadlineTwoCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3266) with Deadline.TwoCompressed with ValueLengthThreeCompressed
          case object DeadlineThreeCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3267) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
          case object DeadlineFourCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3268) with Deadline.FourCompressed with ValueLengthThreeCompressed
          case object DeadlineFiveCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3269) with Deadline.FiveCompressed with ValueLengthThreeCompressed
          case object DeadlineSixCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3270) with Deadline.SixCompressed with ValueLengthThreeCompressed
          case object DeadlineSevenCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3271) with Deadline.SevenCompressed with ValueLengthThreeCompressed
          case object DeadlineFullyCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3272) with Deadline.FullyCompressed with ValueLengthThreeCompressed
          case object DeadlineUncompressed extends UpdateFunctionKeyFullyCompressedEntryId(3273) with Deadline.Uncompressed with ValueLengthThreeCompressed
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
          case object NoDeadline extends UpdateFunctionKeyFullyCompressedEntryId(3274) with Deadline.NoDeadline with ValueLengthFullyCompressed
          case object DeadlineOneCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3275) with Deadline.OneCompressed with ValueLengthFullyCompressed
          case object DeadlineTwoCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3276) with Deadline.TwoCompressed with ValueLengthFullyCompressed
          case object DeadlineThreeCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3277) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
          case object DeadlineFourCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3278) with Deadline.FourCompressed with ValueLengthFullyCompressed
          case object DeadlineFiveCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3279) with Deadline.FiveCompressed with ValueLengthFullyCompressed
          case object DeadlineSixCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3280) with Deadline.SixCompressed with ValueLengthFullyCompressed
          case object DeadlineSevenCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3281) with Deadline.SevenCompressed with ValueLengthFullyCompressed
          case object DeadlineFullyCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3282) with Deadline.FullyCompressed with ValueLengthFullyCompressed
          case object DeadlineUncompressed extends UpdateFunctionKeyFullyCompressedEntryId(3283) with Deadline.Uncompressed with ValueLengthFullyCompressed
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
          case object NoDeadline extends UpdateFunctionKeyFullyCompressedEntryId(3284) with Deadline.NoDeadline with ValueLengthUncompressed
          case object DeadlineOneCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3285) with Deadline.OneCompressed with ValueLengthUncompressed
          case object DeadlineTwoCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3286) with Deadline.TwoCompressed with ValueLengthUncompressed
          case object DeadlineThreeCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3287) with Deadline.ThreeCompressed with ValueLengthUncompressed
          case object DeadlineFourCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3288) with Deadline.FourCompressed with ValueLengthUncompressed
          case object DeadlineFiveCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3289) with Deadline.FiveCompressed with ValueLengthUncompressed
          case object DeadlineSixCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3290) with Deadline.SixCompressed with ValueLengthUncompressed
          case object DeadlineSevenCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3291) with Deadline.SevenCompressed with ValueLengthUncompressed
          case object DeadlineFullyCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3292) with Deadline.FullyCompressed with ValueLengthUncompressed
          case object DeadlineUncompressed extends UpdateFunctionKeyFullyCompressedEntryId(3293) with Deadline.Uncompressed with ValueLengthUncompressed
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
          case object NoDeadline extends UpdateFunctionKeyFullyCompressedEntryId(3294) with Deadline.NoDeadline with ValueLengthOneCompressed
          case object DeadlineOneCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3295) with Deadline.OneCompressed with ValueLengthOneCompressed
          case object DeadlineTwoCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3296) with Deadline.TwoCompressed with ValueLengthOneCompressed
          case object DeadlineThreeCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3297) with Deadline.ThreeCompressed with ValueLengthOneCompressed
          case object DeadlineFourCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3298) with Deadline.FourCompressed with ValueLengthOneCompressed
          case object DeadlineFiveCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3299) with Deadline.FiveCompressed with ValueLengthOneCompressed
          case object DeadlineSixCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3300) with Deadline.SixCompressed with ValueLengthOneCompressed
          case object DeadlineSevenCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3301) with Deadline.SevenCompressed with ValueLengthOneCompressed
          case object DeadlineFullyCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3302) with Deadline.FullyCompressed with ValueLengthOneCompressed
          case object DeadlineUncompressed extends UpdateFunctionKeyFullyCompressedEntryId(3303) with Deadline.Uncompressed with ValueLengthOneCompressed
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
          case object NoDeadline extends UpdateFunctionKeyFullyCompressedEntryId(3304) with Deadline.NoDeadline with ValueLengthTwoCompressed
          case object DeadlineOneCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3305) with Deadline.OneCompressed with ValueLengthTwoCompressed
          case object DeadlineTwoCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3306) with Deadline.TwoCompressed with ValueLengthTwoCompressed
          case object DeadlineThreeCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3307) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
          case object DeadlineFourCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3308) with Deadline.FourCompressed with ValueLengthTwoCompressed
          case object DeadlineFiveCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3309) with Deadline.FiveCompressed with ValueLengthTwoCompressed
          case object DeadlineSixCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3310) with Deadline.SixCompressed with ValueLengthTwoCompressed
          case object DeadlineSevenCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3311) with Deadline.SevenCompressed with ValueLengthTwoCompressed
          case object DeadlineFullyCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3312) with Deadline.FullyCompressed with ValueLengthTwoCompressed
          case object DeadlineUncompressed extends UpdateFunctionKeyFullyCompressedEntryId(3313) with Deadline.Uncompressed with ValueLengthTwoCompressed
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
          case object NoDeadline extends UpdateFunctionKeyFullyCompressedEntryId(3314) with Deadline.NoDeadline with ValueLengthThreeCompressed
          case object DeadlineOneCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3315) with Deadline.OneCompressed with ValueLengthThreeCompressed
          case object DeadlineTwoCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3316) with Deadline.TwoCompressed with ValueLengthThreeCompressed
          case object DeadlineThreeCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3317) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
          case object DeadlineFourCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3318) with Deadline.FourCompressed with ValueLengthThreeCompressed
          case object DeadlineFiveCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3319) with Deadline.FiveCompressed with ValueLengthThreeCompressed
          case object DeadlineSixCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3320) with Deadline.SixCompressed with ValueLengthThreeCompressed
          case object DeadlineSevenCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3321) with Deadline.SevenCompressed with ValueLengthThreeCompressed
          case object DeadlineFullyCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3322) with Deadline.FullyCompressed with ValueLengthThreeCompressed
          case object DeadlineUncompressed extends UpdateFunctionKeyFullyCompressedEntryId(3323) with Deadline.Uncompressed with ValueLengthThreeCompressed
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
          case object NoDeadline extends UpdateFunctionKeyFullyCompressedEntryId(3324) with Deadline.NoDeadline with ValueLengthFullyCompressed
          case object DeadlineOneCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3325) with Deadline.OneCompressed with ValueLengthFullyCompressed
          case object DeadlineTwoCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3326) with Deadline.TwoCompressed with ValueLengthFullyCompressed
          case object DeadlineThreeCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3327) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
          case object DeadlineFourCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3328) with Deadline.FourCompressed with ValueLengthFullyCompressed
          case object DeadlineFiveCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3329) with Deadline.FiveCompressed with ValueLengthFullyCompressed
          case object DeadlineSixCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3330) with Deadline.SixCompressed with ValueLengthFullyCompressed
          case object DeadlineSevenCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3331) with Deadline.SevenCompressed with ValueLengthFullyCompressed
          case object DeadlineFullyCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3332) with Deadline.FullyCompressed with ValueLengthFullyCompressed
          case object DeadlineUncompressed extends UpdateFunctionKeyFullyCompressedEntryId(3333) with Deadline.Uncompressed with ValueLengthFullyCompressed
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
          case object NoDeadline extends UpdateFunctionKeyFullyCompressedEntryId(3334) with Deadline.NoDeadline with ValueLengthUncompressed
          case object DeadlineOneCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3335) with Deadline.OneCompressed with ValueLengthUncompressed
          case object DeadlineTwoCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3336) with Deadline.TwoCompressed with ValueLengthUncompressed
          case object DeadlineThreeCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3337) with Deadline.ThreeCompressed with ValueLengthUncompressed
          case object DeadlineFourCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3338) with Deadline.FourCompressed with ValueLengthUncompressed
          case object DeadlineFiveCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3339) with Deadline.FiveCompressed with ValueLengthUncompressed
          case object DeadlineSixCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3340) with Deadline.SixCompressed with ValueLengthUncompressed
          case object DeadlineSevenCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3341) with Deadline.SevenCompressed with ValueLengthUncompressed
          case object DeadlineFullyCompressed extends UpdateFunctionKeyFullyCompressedEntryId(3342) with Deadline.FullyCompressed with ValueLengthUncompressed
          case object DeadlineUncompressed extends UpdateFunctionKeyFullyCompressedEntryId(3343) with Deadline.Uncompressed with ValueLengthUncompressed
        }
      }
    }
  }
}