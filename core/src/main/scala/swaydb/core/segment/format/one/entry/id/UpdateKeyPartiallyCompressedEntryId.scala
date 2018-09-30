
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
sealed abstract class UpdateKeyPartiallyCompressedEntryId(override val id: Int) extends EntryId(id)
object UpdateKeyPartiallyCompressedEntryId {

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
      case object NoDeadline extends UpdateKeyPartiallyCompressedEntryId(2019) with Deadline.NoDeadline with NoValue
      case object DeadlineOneCompressed extends UpdateKeyPartiallyCompressedEntryId(2020) with Deadline.OneCompressed with NoValue
      case object DeadlineTwoCompressed extends UpdateKeyPartiallyCompressedEntryId(2021) with Deadline.TwoCompressed with NoValue
      case object DeadlineThreeCompressed extends UpdateKeyPartiallyCompressedEntryId(2022) with Deadline.ThreeCompressed with NoValue
      case object DeadlineFourCompressed extends UpdateKeyPartiallyCompressedEntryId(2023) with Deadline.FourCompressed with NoValue
      case object DeadlineFiveCompressed extends UpdateKeyPartiallyCompressedEntryId(2024) with Deadline.FiveCompressed with NoValue
      case object DeadlineSixCompressed extends UpdateKeyPartiallyCompressedEntryId(2025) with Deadline.SixCompressed with NoValue
      case object DeadlineSevenCompressed extends UpdateKeyPartiallyCompressedEntryId(2026) with Deadline.SevenCompressed with NoValue
      case object DeadlineFullyCompressed extends UpdateKeyPartiallyCompressedEntryId(2027) with Deadline.FullyCompressed with NoValue
      case object DeadlineUncompressed extends UpdateKeyPartiallyCompressedEntryId(2028) with Deadline.Uncompressed with NoValue
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
      case object NoDeadline extends UpdateKeyPartiallyCompressedEntryId(2029) with Deadline.NoDeadline with ValueFullyCompressed
      case object DeadlineOneCompressed extends UpdateKeyPartiallyCompressedEntryId(2030) with Deadline.OneCompressed with ValueFullyCompressed
      case object DeadlineTwoCompressed extends UpdateKeyPartiallyCompressedEntryId(2031) with Deadline.TwoCompressed with ValueFullyCompressed
      case object DeadlineThreeCompressed extends UpdateKeyPartiallyCompressedEntryId(2032) with Deadline.ThreeCompressed with ValueFullyCompressed
      case object DeadlineFourCompressed extends UpdateKeyPartiallyCompressedEntryId(2033) with Deadline.FourCompressed with ValueFullyCompressed
      case object DeadlineFiveCompressed extends UpdateKeyPartiallyCompressedEntryId(2034) with Deadline.FiveCompressed with ValueFullyCompressed
      case object DeadlineSixCompressed extends UpdateKeyPartiallyCompressedEntryId(2035) with Deadline.SixCompressed with ValueFullyCompressed
      case object DeadlineSevenCompressed extends UpdateKeyPartiallyCompressedEntryId(2036) with Deadline.SevenCompressed with ValueFullyCompressed
      case object DeadlineFullyCompressed extends UpdateKeyPartiallyCompressedEntryId(2037) with Deadline.FullyCompressed with ValueFullyCompressed
      case object DeadlineUncompressed extends UpdateKeyPartiallyCompressedEntryId(2038) with Deadline.Uncompressed with ValueFullyCompressed
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
          case object NoDeadline extends UpdateKeyPartiallyCompressedEntryId(2039) with Deadline.NoDeadline with ValueLengthOneCompressed
          case object DeadlineOneCompressed extends UpdateKeyPartiallyCompressedEntryId(2040) with Deadline.OneCompressed with ValueLengthOneCompressed
          case object DeadlineTwoCompressed extends UpdateKeyPartiallyCompressedEntryId(2041) with Deadline.TwoCompressed with ValueLengthOneCompressed
          case object DeadlineThreeCompressed extends UpdateKeyPartiallyCompressedEntryId(2042) with Deadline.ThreeCompressed with ValueLengthOneCompressed
          case object DeadlineFourCompressed extends UpdateKeyPartiallyCompressedEntryId(2043) with Deadline.FourCompressed with ValueLengthOneCompressed
          case object DeadlineFiveCompressed extends UpdateKeyPartiallyCompressedEntryId(2044) with Deadline.FiveCompressed with ValueLengthOneCompressed
          case object DeadlineSixCompressed extends UpdateKeyPartiallyCompressedEntryId(2045) with Deadline.SixCompressed with ValueLengthOneCompressed
          case object DeadlineSevenCompressed extends UpdateKeyPartiallyCompressedEntryId(2046) with Deadline.SevenCompressed with ValueLengthOneCompressed
          case object DeadlineFullyCompressed extends UpdateKeyPartiallyCompressedEntryId(2047) with Deadline.FullyCompressed with ValueLengthOneCompressed
          case object DeadlineUncompressed extends UpdateKeyPartiallyCompressedEntryId(2048) with Deadline.Uncompressed with ValueLengthOneCompressed
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
          case object NoDeadline extends UpdateKeyPartiallyCompressedEntryId(2049) with Deadline.NoDeadline with ValueLengthTwoCompressed
          case object DeadlineOneCompressed extends UpdateKeyPartiallyCompressedEntryId(2050) with Deadline.OneCompressed with ValueLengthTwoCompressed
          case object DeadlineTwoCompressed extends UpdateKeyPartiallyCompressedEntryId(2051) with Deadline.TwoCompressed with ValueLengthTwoCompressed
          case object DeadlineThreeCompressed extends UpdateKeyPartiallyCompressedEntryId(2052) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
          case object DeadlineFourCompressed extends UpdateKeyPartiallyCompressedEntryId(2053) with Deadline.FourCompressed with ValueLengthTwoCompressed
          case object DeadlineFiveCompressed extends UpdateKeyPartiallyCompressedEntryId(2054) with Deadline.FiveCompressed with ValueLengthTwoCompressed
          case object DeadlineSixCompressed extends UpdateKeyPartiallyCompressedEntryId(2055) with Deadline.SixCompressed with ValueLengthTwoCompressed
          case object DeadlineSevenCompressed extends UpdateKeyPartiallyCompressedEntryId(2056) with Deadline.SevenCompressed with ValueLengthTwoCompressed
          case object DeadlineFullyCompressed extends UpdateKeyPartiallyCompressedEntryId(2057) with Deadline.FullyCompressed with ValueLengthTwoCompressed
          case object DeadlineUncompressed extends UpdateKeyPartiallyCompressedEntryId(2058) with Deadline.Uncompressed with ValueLengthTwoCompressed
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
          case object NoDeadline extends UpdateKeyPartiallyCompressedEntryId(2059) with Deadline.NoDeadline with ValueLengthThreeCompressed
          case object DeadlineOneCompressed extends UpdateKeyPartiallyCompressedEntryId(2060) with Deadline.OneCompressed with ValueLengthThreeCompressed
          case object DeadlineTwoCompressed extends UpdateKeyPartiallyCompressedEntryId(2061) with Deadline.TwoCompressed with ValueLengthThreeCompressed
          case object DeadlineThreeCompressed extends UpdateKeyPartiallyCompressedEntryId(2062) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
          case object DeadlineFourCompressed extends UpdateKeyPartiallyCompressedEntryId(2063) with Deadline.FourCompressed with ValueLengthThreeCompressed
          case object DeadlineFiveCompressed extends UpdateKeyPartiallyCompressedEntryId(2064) with Deadline.FiveCompressed with ValueLengthThreeCompressed
          case object DeadlineSixCompressed extends UpdateKeyPartiallyCompressedEntryId(2065) with Deadline.SixCompressed with ValueLengthThreeCompressed
          case object DeadlineSevenCompressed extends UpdateKeyPartiallyCompressedEntryId(2066) with Deadline.SevenCompressed with ValueLengthThreeCompressed
          case object DeadlineFullyCompressed extends UpdateKeyPartiallyCompressedEntryId(2067) with Deadline.FullyCompressed with ValueLengthThreeCompressed
          case object DeadlineUncompressed extends UpdateKeyPartiallyCompressedEntryId(2068) with Deadline.Uncompressed with ValueLengthThreeCompressed
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
          case object NoDeadline extends UpdateKeyPartiallyCompressedEntryId(2069) with Deadline.NoDeadline with ValueLengthFullyCompressed
          case object DeadlineOneCompressed extends UpdateKeyPartiallyCompressedEntryId(2070) with Deadline.OneCompressed with ValueLengthFullyCompressed
          case object DeadlineTwoCompressed extends UpdateKeyPartiallyCompressedEntryId(2071) with Deadline.TwoCompressed with ValueLengthFullyCompressed
          case object DeadlineThreeCompressed extends UpdateKeyPartiallyCompressedEntryId(2072) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
          case object DeadlineFourCompressed extends UpdateKeyPartiallyCompressedEntryId(2073) with Deadline.FourCompressed with ValueLengthFullyCompressed
          case object DeadlineFiveCompressed extends UpdateKeyPartiallyCompressedEntryId(2074) with Deadline.FiveCompressed with ValueLengthFullyCompressed
          case object DeadlineSixCompressed extends UpdateKeyPartiallyCompressedEntryId(2075) with Deadline.SixCompressed with ValueLengthFullyCompressed
          case object DeadlineSevenCompressed extends UpdateKeyPartiallyCompressedEntryId(2076) with Deadline.SevenCompressed with ValueLengthFullyCompressed
          case object DeadlineFullyCompressed extends UpdateKeyPartiallyCompressedEntryId(2077) with Deadline.FullyCompressed with ValueLengthFullyCompressed
          case object DeadlineUncompressed extends UpdateKeyPartiallyCompressedEntryId(2078) with Deadline.Uncompressed with ValueLengthFullyCompressed
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
          case object NoDeadline extends UpdateKeyPartiallyCompressedEntryId(2079) with Deadline.NoDeadline with ValueLengthUncompressed
          case object DeadlineOneCompressed extends UpdateKeyPartiallyCompressedEntryId(2080) with Deadline.OneCompressed with ValueLengthUncompressed
          case object DeadlineTwoCompressed extends UpdateKeyPartiallyCompressedEntryId(2081) with Deadline.TwoCompressed with ValueLengthUncompressed
          case object DeadlineThreeCompressed extends UpdateKeyPartiallyCompressedEntryId(2082) with Deadline.ThreeCompressed with ValueLengthUncompressed
          case object DeadlineFourCompressed extends UpdateKeyPartiallyCompressedEntryId(2083) with Deadline.FourCompressed with ValueLengthUncompressed
          case object DeadlineFiveCompressed extends UpdateKeyPartiallyCompressedEntryId(2084) with Deadline.FiveCompressed with ValueLengthUncompressed
          case object DeadlineSixCompressed extends UpdateKeyPartiallyCompressedEntryId(2085) with Deadline.SixCompressed with ValueLengthUncompressed
          case object DeadlineSevenCompressed extends UpdateKeyPartiallyCompressedEntryId(2086) with Deadline.SevenCompressed with ValueLengthUncompressed
          case object DeadlineFullyCompressed extends UpdateKeyPartiallyCompressedEntryId(2087) with Deadline.FullyCompressed with ValueLengthUncompressed
          case object DeadlineUncompressed extends UpdateKeyPartiallyCompressedEntryId(2088) with Deadline.Uncompressed with ValueLengthUncompressed
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
          case object NoDeadline extends UpdateKeyPartiallyCompressedEntryId(2089) with Deadline.NoDeadline with ValueLengthOneCompressed
          case object DeadlineOneCompressed extends UpdateKeyPartiallyCompressedEntryId(2090) with Deadline.OneCompressed with ValueLengthOneCompressed
          case object DeadlineTwoCompressed extends UpdateKeyPartiallyCompressedEntryId(2091) with Deadline.TwoCompressed with ValueLengthOneCompressed
          case object DeadlineThreeCompressed extends UpdateKeyPartiallyCompressedEntryId(2092) with Deadline.ThreeCompressed with ValueLengthOneCompressed
          case object DeadlineFourCompressed extends UpdateKeyPartiallyCompressedEntryId(2093) with Deadline.FourCompressed with ValueLengthOneCompressed
          case object DeadlineFiveCompressed extends UpdateKeyPartiallyCompressedEntryId(2094) with Deadline.FiveCompressed with ValueLengthOneCompressed
          case object DeadlineSixCompressed extends UpdateKeyPartiallyCompressedEntryId(2095) with Deadline.SixCompressed with ValueLengthOneCompressed
          case object DeadlineSevenCompressed extends UpdateKeyPartiallyCompressedEntryId(2096) with Deadline.SevenCompressed with ValueLengthOneCompressed
          case object DeadlineFullyCompressed extends UpdateKeyPartiallyCompressedEntryId(2097) with Deadline.FullyCompressed with ValueLengthOneCompressed
          case object DeadlineUncompressed extends UpdateKeyPartiallyCompressedEntryId(2098) with Deadline.Uncompressed with ValueLengthOneCompressed
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
          case object NoDeadline extends UpdateKeyPartiallyCompressedEntryId(2099) with Deadline.NoDeadline with ValueLengthTwoCompressed
          case object DeadlineOneCompressed extends UpdateKeyPartiallyCompressedEntryId(2100) with Deadline.OneCompressed with ValueLengthTwoCompressed
          case object DeadlineTwoCompressed extends UpdateKeyPartiallyCompressedEntryId(2101) with Deadline.TwoCompressed with ValueLengthTwoCompressed
          case object DeadlineThreeCompressed extends UpdateKeyPartiallyCompressedEntryId(2102) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
          case object DeadlineFourCompressed extends UpdateKeyPartiallyCompressedEntryId(2103) with Deadline.FourCompressed with ValueLengthTwoCompressed
          case object DeadlineFiveCompressed extends UpdateKeyPartiallyCompressedEntryId(2104) with Deadline.FiveCompressed with ValueLengthTwoCompressed
          case object DeadlineSixCompressed extends UpdateKeyPartiallyCompressedEntryId(2105) with Deadline.SixCompressed with ValueLengthTwoCompressed
          case object DeadlineSevenCompressed extends UpdateKeyPartiallyCompressedEntryId(2106) with Deadline.SevenCompressed with ValueLengthTwoCompressed
          case object DeadlineFullyCompressed extends UpdateKeyPartiallyCompressedEntryId(2107) with Deadline.FullyCompressed with ValueLengthTwoCompressed
          case object DeadlineUncompressed extends UpdateKeyPartiallyCompressedEntryId(2108) with Deadline.Uncompressed with ValueLengthTwoCompressed
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
          case object NoDeadline extends UpdateKeyPartiallyCompressedEntryId(2109) with Deadline.NoDeadline with ValueLengthThreeCompressed
          case object DeadlineOneCompressed extends UpdateKeyPartiallyCompressedEntryId(2110) with Deadline.OneCompressed with ValueLengthThreeCompressed
          case object DeadlineTwoCompressed extends UpdateKeyPartiallyCompressedEntryId(2111) with Deadline.TwoCompressed with ValueLengthThreeCompressed
          case object DeadlineThreeCompressed extends UpdateKeyPartiallyCompressedEntryId(2112) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
          case object DeadlineFourCompressed extends UpdateKeyPartiallyCompressedEntryId(2113) with Deadline.FourCompressed with ValueLengthThreeCompressed
          case object DeadlineFiveCompressed extends UpdateKeyPartiallyCompressedEntryId(2114) with Deadline.FiveCompressed with ValueLengthThreeCompressed
          case object DeadlineSixCompressed extends UpdateKeyPartiallyCompressedEntryId(2115) with Deadline.SixCompressed with ValueLengthThreeCompressed
          case object DeadlineSevenCompressed extends UpdateKeyPartiallyCompressedEntryId(2116) with Deadline.SevenCompressed with ValueLengthThreeCompressed
          case object DeadlineFullyCompressed extends UpdateKeyPartiallyCompressedEntryId(2117) with Deadline.FullyCompressed with ValueLengthThreeCompressed
          case object DeadlineUncompressed extends UpdateKeyPartiallyCompressedEntryId(2118) with Deadline.Uncompressed with ValueLengthThreeCompressed
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
          case object NoDeadline extends UpdateKeyPartiallyCompressedEntryId(2119) with Deadline.NoDeadline with ValueLengthFullyCompressed
          case object DeadlineOneCompressed extends UpdateKeyPartiallyCompressedEntryId(2120) with Deadline.OneCompressed with ValueLengthFullyCompressed
          case object DeadlineTwoCompressed extends UpdateKeyPartiallyCompressedEntryId(2121) with Deadline.TwoCompressed with ValueLengthFullyCompressed
          case object DeadlineThreeCompressed extends UpdateKeyPartiallyCompressedEntryId(2122) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
          case object DeadlineFourCompressed extends UpdateKeyPartiallyCompressedEntryId(2123) with Deadline.FourCompressed with ValueLengthFullyCompressed
          case object DeadlineFiveCompressed extends UpdateKeyPartiallyCompressedEntryId(2124) with Deadline.FiveCompressed with ValueLengthFullyCompressed
          case object DeadlineSixCompressed extends UpdateKeyPartiallyCompressedEntryId(2125) with Deadline.SixCompressed with ValueLengthFullyCompressed
          case object DeadlineSevenCompressed extends UpdateKeyPartiallyCompressedEntryId(2126) with Deadline.SevenCompressed with ValueLengthFullyCompressed
          case object DeadlineFullyCompressed extends UpdateKeyPartiallyCompressedEntryId(2127) with Deadline.FullyCompressed with ValueLengthFullyCompressed
          case object DeadlineUncompressed extends UpdateKeyPartiallyCompressedEntryId(2128) with Deadline.Uncompressed with ValueLengthFullyCompressed
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
          case object NoDeadline extends UpdateKeyPartiallyCompressedEntryId(2129) with Deadline.NoDeadline with ValueLengthUncompressed
          case object DeadlineOneCompressed extends UpdateKeyPartiallyCompressedEntryId(2130) with Deadline.OneCompressed with ValueLengthUncompressed
          case object DeadlineTwoCompressed extends UpdateKeyPartiallyCompressedEntryId(2131) with Deadline.TwoCompressed with ValueLengthUncompressed
          case object DeadlineThreeCompressed extends UpdateKeyPartiallyCompressedEntryId(2132) with Deadline.ThreeCompressed with ValueLengthUncompressed
          case object DeadlineFourCompressed extends UpdateKeyPartiallyCompressedEntryId(2133) with Deadline.FourCompressed with ValueLengthUncompressed
          case object DeadlineFiveCompressed extends UpdateKeyPartiallyCompressedEntryId(2134) with Deadline.FiveCompressed with ValueLengthUncompressed
          case object DeadlineSixCompressed extends UpdateKeyPartiallyCompressedEntryId(2135) with Deadline.SixCompressed with ValueLengthUncompressed
          case object DeadlineSevenCompressed extends UpdateKeyPartiallyCompressedEntryId(2136) with Deadline.SevenCompressed with ValueLengthUncompressed
          case object DeadlineFullyCompressed extends UpdateKeyPartiallyCompressedEntryId(2137) with Deadline.FullyCompressed with ValueLengthUncompressed
          case object DeadlineUncompressed extends UpdateKeyPartiallyCompressedEntryId(2138) with Deadline.Uncompressed with ValueLengthUncompressed
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
          case object NoDeadline extends UpdateKeyPartiallyCompressedEntryId(2139) with Deadline.NoDeadline with ValueLengthOneCompressed
          case object DeadlineOneCompressed extends UpdateKeyPartiallyCompressedEntryId(2140) with Deadline.OneCompressed with ValueLengthOneCompressed
          case object DeadlineTwoCompressed extends UpdateKeyPartiallyCompressedEntryId(2141) with Deadline.TwoCompressed with ValueLengthOneCompressed
          case object DeadlineThreeCompressed extends UpdateKeyPartiallyCompressedEntryId(2142) with Deadline.ThreeCompressed with ValueLengthOneCompressed
          case object DeadlineFourCompressed extends UpdateKeyPartiallyCompressedEntryId(2143) with Deadline.FourCompressed with ValueLengthOneCompressed
          case object DeadlineFiveCompressed extends UpdateKeyPartiallyCompressedEntryId(2144) with Deadline.FiveCompressed with ValueLengthOneCompressed
          case object DeadlineSixCompressed extends UpdateKeyPartiallyCompressedEntryId(2145) with Deadline.SixCompressed with ValueLengthOneCompressed
          case object DeadlineSevenCompressed extends UpdateKeyPartiallyCompressedEntryId(2146) with Deadline.SevenCompressed with ValueLengthOneCompressed
          case object DeadlineFullyCompressed extends UpdateKeyPartiallyCompressedEntryId(2147) with Deadline.FullyCompressed with ValueLengthOneCompressed
          case object DeadlineUncompressed extends UpdateKeyPartiallyCompressedEntryId(2148) with Deadline.Uncompressed with ValueLengthOneCompressed
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
          case object NoDeadline extends UpdateKeyPartiallyCompressedEntryId(2149) with Deadline.NoDeadline with ValueLengthTwoCompressed
          case object DeadlineOneCompressed extends UpdateKeyPartiallyCompressedEntryId(2150) with Deadline.OneCompressed with ValueLengthTwoCompressed
          case object DeadlineTwoCompressed extends UpdateKeyPartiallyCompressedEntryId(2151) with Deadline.TwoCompressed with ValueLengthTwoCompressed
          case object DeadlineThreeCompressed extends UpdateKeyPartiallyCompressedEntryId(2152) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
          case object DeadlineFourCompressed extends UpdateKeyPartiallyCompressedEntryId(2153) with Deadline.FourCompressed with ValueLengthTwoCompressed
          case object DeadlineFiveCompressed extends UpdateKeyPartiallyCompressedEntryId(2154) with Deadline.FiveCompressed with ValueLengthTwoCompressed
          case object DeadlineSixCompressed extends UpdateKeyPartiallyCompressedEntryId(2155) with Deadline.SixCompressed with ValueLengthTwoCompressed
          case object DeadlineSevenCompressed extends UpdateKeyPartiallyCompressedEntryId(2156) with Deadline.SevenCompressed with ValueLengthTwoCompressed
          case object DeadlineFullyCompressed extends UpdateKeyPartiallyCompressedEntryId(2157) with Deadline.FullyCompressed with ValueLengthTwoCompressed
          case object DeadlineUncompressed extends UpdateKeyPartiallyCompressedEntryId(2158) with Deadline.Uncompressed with ValueLengthTwoCompressed
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
          case object NoDeadline extends UpdateKeyPartiallyCompressedEntryId(2159) with Deadline.NoDeadline with ValueLengthThreeCompressed
          case object DeadlineOneCompressed extends UpdateKeyPartiallyCompressedEntryId(2160) with Deadline.OneCompressed with ValueLengthThreeCompressed
          case object DeadlineTwoCompressed extends UpdateKeyPartiallyCompressedEntryId(2161) with Deadline.TwoCompressed with ValueLengthThreeCompressed
          case object DeadlineThreeCompressed extends UpdateKeyPartiallyCompressedEntryId(2162) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
          case object DeadlineFourCompressed extends UpdateKeyPartiallyCompressedEntryId(2163) with Deadline.FourCompressed with ValueLengthThreeCompressed
          case object DeadlineFiveCompressed extends UpdateKeyPartiallyCompressedEntryId(2164) with Deadline.FiveCompressed with ValueLengthThreeCompressed
          case object DeadlineSixCompressed extends UpdateKeyPartiallyCompressedEntryId(2165) with Deadline.SixCompressed with ValueLengthThreeCompressed
          case object DeadlineSevenCompressed extends UpdateKeyPartiallyCompressedEntryId(2166) with Deadline.SevenCompressed with ValueLengthThreeCompressed
          case object DeadlineFullyCompressed extends UpdateKeyPartiallyCompressedEntryId(2167) with Deadline.FullyCompressed with ValueLengthThreeCompressed
          case object DeadlineUncompressed extends UpdateKeyPartiallyCompressedEntryId(2168) with Deadline.Uncompressed with ValueLengthThreeCompressed
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
          case object NoDeadline extends UpdateKeyPartiallyCompressedEntryId(2169) with Deadline.NoDeadline with ValueLengthFullyCompressed
          case object DeadlineOneCompressed extends UpdateKeyPartiallyCompressedEntryId(2170) with Deadline.OneCompressed with ValueLengthFullyCompressed
          case object DeadlineTwoCompressed extends UpdateKeyPartiallyCompressedEntryId(2171) with Deadline.TwoCompressed with ValueLengthFullyCompressed
          case object DeadlineThreeCompressed extends UpdateKeyPartiallyCompressedEntryId(2172) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
          case object DeadlineFourCompressed extends UpdateKeyPartiallyCompressedEntryId(2173) with Deadline.FourCompressed with ValueLengthFullyCompressed
          case object DeadlineFiveCompressed extends UpdateKeyPartiallyCompressedEntryId(2174) with Deadline.FiveCompressed with ValueLengthFullyCompressed
          case object DeadlineSixCompressed extends UpdateKeyPartiallyCompressedEntryId(2175) with Deadline.SixCompressed with ValueLengthFullyCompressed
          case object DeadlineSevenCompressed extends UpdateKeyPartiallyCompressedEntryId(2176) with Deadline.SevenCompressed with ValueLengthFullyCompressed
          case object DeadlineFullyCompressed extends UpdateKeyPartiallyCompressedEntryId(2177) with Deadline.FullyCompressed with ValueLengthFullyCompressed
          case object DeadlineUncompressed extends UpdateKeyPartiallyCompressedEntryId(2178) with Deadline.Uncompressed with ValueLengthFullyCompressed
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
          case object NoDeadline extends UpdateKeyPartiallyCompressedEntryId(2179) with Deadline.NoDeadline with ValueLengthUncompressed
          case object DeadlineOneCompressed extends UpdateKeyPartiallyCompressedEntryId(2180) with Deadline.OneCompressed with ValueLengthUncompressed
          case object DeadlineTwoCompressed extends UpdateKeyPartiallyCompressedEntryId(2181) with Deadline.TwoCompressed with ValueLengthUncompressed
          case object DeadlineThreeCompressed extends UpdateKeyPartiallyCompressedEntryId(2182) with Deadline.ThreeCompressed with ValueLengthUncompressed
          case object DeadlineFourCompressed extends UpdateKeyPartiallyCompressedEntryId(2183) with Deadline.FourCompressed with ValueLengthUncompressed
          case object DeadlineFiveCompressed extends UpdateKeyPartiallyCompressedEntryId(2184) with Deadline.FiveCompressed with ValueLengthUncompressed
          case object DeadlineSixCompressed extends UpdateKeyPartiallyCompressedEntryId(2185) with Deadline.SixCompressed with ValueLengthUncompressed
          case object DeadlineSevenCompressed extends UpdateKeyPartiallyCompressedEntryId(2186) with Deadline.SevenCompressed with ValueLengthUncompressed
          case object DeadlineFullyCompressed extends UpdateKeyPartiallyCompressedEntryId(2187) with Deadline.FullyCompressed with ValueLengthUncompressed
          case object DeadlineUncompressed extends UpdateKeyPartiallyCompressedEntryId(2188) with Deadline.Uncompressed with ValueLengthUncompressed
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
          case object NoDeadline extends UpdateKeyPartiallyCompressedEntryId(2189) with Deadline.NoDeadline with ValueLengthOneCompressed
          case object DeadlineOneCompressed extends UpdateKeyPartiallyCompressedEntryId(2190) with Deadline.OneCompressed with ValueLengthOneCompressed
          case object DeadlineTwoCompressed extends UpdateKeyPartiallyCompressedEntryId(2191) with Deadline.TwoCompressed with ValueLengthOneCompressed
          case object DeadlineThreeCompressed extends UpdateKeyPartiallyCompressedEntryId(2192) with Deadline.ThreeCompressed with ValueLengthOneCompressed
          case object DeadlineFourCompressed extends UpdateKeyPartiallyCompressedEntryId(2193) with Deadline.FourCompressed with ValueLengthOneCompressed
          case object DeadlineFiveCompressed extends UpdateKeyPartiallyCompressedEntryId(2194) with Deadline.FiveCompressed with ValueLengthOneCompressed
          case object DeadlineSixCompressed extends UpdateKeyPartiallyCompressedEntryId(2195) with Deadline.SixCompressed with ValueLengthOneCompressed
          case object DeadlineSevenCompressed extends UpdateKeyPartiallyCompressedEntryId(2196) with Deadline.SevenCompressed with ValueLengthOneCompressed
          case object DeadlineFullyCompressed extends UpdateKeyPartiallyCompressedEntryId(2197) with Deadline.FullyCompressed with ValueLengthOneCompressed
          case object DeadlineUncompressed extends UpdateKeyPartiallyCompressedEntryId(2198) with Deadline.Uncompressed with ValueLengthOneCompressed
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
          case object NoDeadline extends UpdateKeyPartiallyCompressedEntryId(2199) with Deadline.NoDeadline with ValueLengthTwoCompressed
          case object DeadlineOneCompressed extends UpdateKeyPartiallyCompressedEntryId(2200) with Deadline.OneCompressed with ValueLengthTwoCompressed
          case object DeadlineTwoCompressed extends UpdateKeyPartiallyCompressedEntryId(2201) with Deadline.TwoCompressed with ValueLengthTwoCompressed
          case object DeadlineThreeCompressed extends UpdateKeyPartiallyCompressedEntryId(2202) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
          case object DeadlineFourCompressed extends UpdateKeyPartiallyCompressedEntryId(2203) with Deadline.FourCompressed with ValueLengthTwoCompressed
          case object DeadlineFiveCompressed extends UpdateKeyPartiallyCompressedEntryId(2204) with Deadline.FiveCompressed with ValueLengthTwoCompressed
          case object DeadlineSixCompressed extends UpdateKeyPartiallyCompressedEntryId(2205) with Deadline.SixCompressed with ValueLengthTwoCompressed
          case object DeadlineSevenCompressed extends UpdateKeyPartiallyCompressedEntryId(2206) with Deadline.SevenCompressed with ValueLengthTwoCompressed
          case object DeadlineFullyCompressed extends UpdateKeyPartiallyCompressedEntryId(2207) with Deadline.FullyCompressed with ValueLengthTwoCompressed
          case object DeadlineUncompressed extends UpdateKeyPartiallyCompressedEntryId(2208) with Deadline.Uncompressed with ValueLengthTwoCompressed
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
          case object NoDeadline extends UpdateKeyPartiallyCompressedEntryId(2209) with Deadline.NoDeadline with ValueLengthThreeCompressed
          case object DeadlineOneCompressed extends UpdateKeyPartiallyCompressedEntryId(2210) with Deadline.OneCompressed with ValueLengthThreeCompressed
          case object DeadlineTwoCompressed extends UpdateKeyPartiallyCompressedEntryId(2211) with Deadline.TwoCompressed with ValueLengthThreeCompressed
          case object DeadlineThreeCompressed extends UpdateKeyPartiallyCompressedEntryId(2212) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
          case object DeadlineFourCompressed extends UpdateKeyPartiallyCompressedEntryId(2213) with Deadline.FourCompressed with ValueLengthThreeCompressed
          case object DeadlineFiveCompressed extends UpdateKeyPartiallyCompressedEntryId(2214) with Deadline.FiveCompressed with ValueLengthThreeCompressed
          case object DeadlineSixCompressed extends UpdateKeyPartiallyCompressedEntryId(2215) with Deadline.SixCompressed with ValueLengthThreeCompressed
          case object DeadlineSevenCompressed extends UpdateKeyPartiallyCompressedEntryId(2216) with Deadline.SevenCompressed with ValueLengthThreeCompressed
          case object DeadlineFullyCompressed extends UpdateKeyPartiallyCompressedEntryId(2217) with Deadline.FullyCompressed with ValueLengthThreeCompressed
          case object DeadlineUncompressed extends UpdateKeyPartiallyCompressedEntryId(2218) with Deadline.Uncompressed with ValueLengthThreeCompressed
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
          case object NoDeadline extends UpdateKeyPartiallyCompressedEntryId(2219) with Deadline.NoDeadline with ValueLengthFullyCompressed
          case object DeadlineOneCompressed extends UpdateKeyPartiallyCompressedEntryId(2220) with Deadline.OneCompressed with ValueLengthFullyCompressed
          case object DeadlineTwoCompressed extends UpdateKeyPartiallyCompressedEntryId(2221) with Deadline.TwoCompressed with ValueLengthFullyCompressed
          case object DeadlineThreeCompressed extends UpdateKeyPartiallyCompressedEntryId(2222) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
          case object DeadlineFourCompressed extends UpdateKeyPartiallyCompressedEntryId(2223) with Deadline.FourCompressed with ValueLengthFullyCompressed
          case object DeadlineFiveCompressed extends UpdateKeyPartiallyCompressedEntryId(2224) with Deadline.FiveCompressed with ValueLengthFullyCompressed
          case object DeadlineSixCompressed extends UpdateKeyPartiallyCompressedEntryId(2225) with Deadline.SixCompressed with ValueLengthFullyCompressed
          case object DeadlineSevenCompressed extends UpdateKeyPartiallyCompressedEntryId(2226) with Deadline.SevenCompressed with ValueLengthFullyCompressed
          case object DeadlineFullyCompressed extends UpdateKeyPartiallyCompressedEntryId(2227) with Deadline.FullyCompressed with ValueLengthFullyCompressed
          case object DeadlineUncompressed extends UpdateKeyPartiallyCompressedEntryId(2228) with Deadline.Uncompressed with ValueLengthFullyCompressed
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
          case object NoDeadline extends UpdateKeyPartiallyCompressedEntryId(2229) with Deadline.NoDeadline with ValueLengthUncompressed
          case object DeadlineOneCompressed extends UpdateKeyPartiallyCompressedEntryId(2230) with Deadline.OneCompressed with ValueLengthUncompressed
          case object DeadlineTwoCompressed extends UpdateKeyPartiallyCompressedEntryId(2231) with Deadline.TwoCompressed with ValueLengthUncompressed
          case object DeadlineThreeCompressed extends UpdateKeyPartiallyCompressedEntryId(2232) with Deadline.ThreeCompressed with ValueLengthUncompressed
          case object DeadlineFourCompressed extends UpdateKeyPartiallyCompressedEntryId(2233) with Deadline.FourCompressed with ValueLengthUncompressed
          case object DeadlineFiveCompressed extends UpdateKeyPartiallyCompressedEntryId(2234) with Deadline.FiveCompressed with ValueLengthUncompressed
          case object DeadlineSixCompressed extends UpdateKeyPartiallyCompressedEntryId(2235) with Deadline.SixCompressed with ValueLengthUncompressed
          case object DeadlineSevenCompressed extends UpdateKeyPartiallyCompressedEntryId(2236) with Deadline.SevenCompressed with ValueLengthUncompressed
          case object DeadlineFullyCompressed extends UpdateKeyPartiallyCompressedEntryId(2237) with Deadline.FullyCompressed with ValueLengthUncompressed
          case object DeadlineUncompressed extends UpdateKeyPartiallyCompressedEntryId(2238) with Deadline.Uncompressed with ValueLengthUncompressed
        }
      }
    }
  }

  def keyIdsList: List[UpdateKeyPartiallyCompressedEntryId] = SealedList.list[UpdateKeyPartiallyCompressedEntryId].sortBy(_.id)

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