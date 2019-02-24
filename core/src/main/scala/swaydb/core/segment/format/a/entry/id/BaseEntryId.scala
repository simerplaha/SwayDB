
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

import swaydb.core.segment.format.a.entry.id.EntryId._
import swaydb.macros.SealedList

sealed abstract class BaseEntryId(override val id: Int) extends EntryId(id)
object BaseEntryId extends EntryFormat {

  override def format: EntryId.Format = BaseEntryId.FormatA
  trait FormatA extends Format.A {
    override def keyPartiallyCompressed: Key.PartiallyCompressed = FormatA.KeyPartiallyCompressed
    override def keyFullyCompressed: Key.FullyCompressed = FormatA.KeyFullyCompressed
    override def keyUncompressed: Key.Uncompressed = FormatA.KeyUncompressed
  }
  object FormatA extends FormatA {

    trait KeyPartiallyCompressed extends Key.PartiallyCompressed with FormatA {
      override def noTime: Time.NoTime = KeyPartiallyCompressed.NoTime
      override def timeFullyCompressed: Time.FullyCompressed = KeyPartiallyCompressed.TimeFullyCompressed
      override def timePartiallyCompressed: Time.PartiallyCompressed = KeyPartiallyCompressed.TimePartiallyCompressed
      override def timeUncompressed: Time.Uncompressed = KeyPartiallyCompressed.TimeUncompressed
    }
    object KeyPartiallyCompressed extends KeyPartiallyCompressed {

      trait TimePartiallyCompressed extends Time.PartiallyCompressed with KeyPartiallyCompressed {
        override def noValue: Value.NoValue = TimePartiallyCompressed.NoValue
        override def valueFullyCompressed: Value.FullyCompressed = TimePartiallyCompressed.ValueFullyCompressed
        override def valueUncompressed: Value.Uncompressed = TimePartiallyCompressed.ValueUncompressed
      }
      object TimePartiallyCompressed extends TimePartiallyCompressed {

        trait ValueUncompressed extends Value.Uncompressed with TimePartiallyCompressed {
          override def valueOffsetOneCompressed: ValueOffset.OneCompressed = ValueUncompressed.ValueOffsetOneCompressed
          override def valueOffsetTwoCompressed: ValueOffset.TwoCompressed = ValueUncompressed.ValueOffsetTwoCompressed
          override def valueOffsetThreeCompressed: ValueOffset.ThreeCompressed = ValueUncompressed.ValueOffsetThreeCompressed
          override def valueOffsetUncompressed: ValueOffset.Uncompressed = ValueUncompressed.ValueOffsetUncompressed
        }
        object ValueUncompressed extends ValueUncompressed {

          trait ValueOffsetOneCompressed extends ValueOffset.OneCompressed with ValueUncompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetOneCompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetOneCompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetOneCompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetOneCompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetOneCompressed.ValueLengthUncompressed
          }
          object ValueOffsetOneCompressed extends ValueOffsetOneCompressed {
            trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetOneCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthOneCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthOneCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthOneCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthOneCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthOneCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthOneCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthOneCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthOneCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthOneCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthOneCompressed.DeadlineUncompressed
            }

            object ValueLengthOneCompressed extends ValueLengthOneCompressed {
              object NoDeadline extends BaseEntryId(0) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryId(1) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryId(2) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryId(3) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryId(4) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryId(5) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryId(6) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryId(7) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryId(8) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryId(9) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetOneCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthTwoCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthTwoCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthTwoCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthTwoCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthTwoCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthTwoCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthTwoCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthTwoCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthTwoCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthTwoCompressed.DeadlineUncompressed
            }

            object ValueLengthTwoCompressed extends ValueLengthTwoCompressed {
              object NoDeadline extends BaseEntryId(10) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryId(11) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryId(12) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryId(13) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryId(14) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryId(15) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryId(16) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryId(17) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryId(18) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryId(19) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetOneCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthThreeCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthThreeCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthThreeCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthThreeCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthThreeCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthThreeCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthThreeCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthThreeCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthThreeCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthThreeCompressed.DeadlineUncompressed
            }

            object ValueLengthThreeCompressed extends ValueLengthThreeCompressed {
              object NoDeadline extends BaseEntryId(20) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryId(21) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryId(22) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryId(23) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryId(24) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryId(25) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryId(26) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryId(27) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryId(28) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryId(29) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetOneCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthFullyCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthFullyCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthFullyCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthFullyCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthFullyCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthFullyCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthFullyCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthFullyCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthFullyCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthFullyCompressed.DeadlineUncompressed
            }

            object ValueLengthFullyCompressed extends ValueLengthFullyCompressed {
              object NoDeadline extends BaseEntryId(30) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryId(31) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryId(32) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryId(33) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryId(34) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryId(35) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryId(36) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryId(37) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryId(38) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryId(39) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetOneCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthUncompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthUncompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthUncompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthUncompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthUncompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthUncompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthUncompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthUncompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthUncompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthUncompressed.DeadlineUncompressed
            }

            object ValueLengthUncompressed extends ValueLengthUncompressed {
              object NoDeadline extends BaseEntryId(40) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryId(41) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryId(42) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryId(43) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryId(44) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryId(45) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryId(46) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryId(47) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryId(48) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryId(49) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }

          trait ValueOffsetTwoCompressed extends ValueOffset.TwoCompressed with ValueUncompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetTwoCompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetTwoCompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetTwoCompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetTwoCompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetTwoCompressed.ValueLengthUncompressed
          }
          object ValueOffsetTwoCompressed extends ValueOffsetTwoCompressed {
            trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetTwoCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthOneCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthOneCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthOneCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthOneCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthOneCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthOneCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthOneCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthOneCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthOneCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthOneCompressed.DeadlineUncompressed
            }

            object ValueLengthOneCompressed extends ValueLengthOneCompressed {
              object NoDeadline extends BaseEntryId(50) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryId(51) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryId(52) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryId(53) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryId(54) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryId(55) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryId(56) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryId(57) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryId(58) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryId(59) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetTwoCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthTwoCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthTwoCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthTwoCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthTwoCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthTwoCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthTwoCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthTwoCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthTwoCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthTwoCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthTwoCompressed.DeadlineUncompressed
            }

            object ValueLengthTwoCompressed extends ValueLengthTwoCompressed {
              object NoDeadline extends BaseEntryId(60) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryId(61) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryId(62) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryId(63) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryId(64) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryId(65) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryId(66) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryId(67) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryId(68) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryId(69) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetTwoCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthThreeCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthThreeCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthThreeCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthThreeCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthThreeCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthThreeCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthThreeCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthThreeCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthThreeCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthThreeCompressed.DeadlineUncompressed
            }

            object ValueLengthThreeCompressed extends ValueLengthThreeCompressed {
              object NoDeadline extends BaseEntryId(70) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryId(71) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryId(72) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryId(73) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryId(74) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryId(75) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryId(76) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryId(77) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryId(78) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryId(79) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetTwoCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthFullyCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthFullyCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthFullyCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthFullyCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthFullyCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthFullyCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthFullyCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthFullyCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthFullyCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthFullyCompressed.DeadlineUncompressed
            }

            object ValueLengthFullyCompressed extends ValueLengthFullyCompressed {
              object NoDeadline extends BaseEntryId(80) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryId(81) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryId(82) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryId(83) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryId(84) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryId(85) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryId(86) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryId(87) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryId(88) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryId(89) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetTwoCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthUncompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthUncompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthUncompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthUncompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthUncompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthUncompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthUncompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthUncompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthUncompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthUncompressed.DeadlineUncompressed
            }

            object ValueLengthUncompressed extends ValueLengthUncompressed {
              object NoDeadline extends BaseEntryId(90) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryId(91) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryId(92) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryId(93) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryId(94) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryId(95) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryId(96) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryId(97) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryId(98) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryId(99) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }

          trait ValueOffsetThreeCompressed extends ValueOffset.ThreeCompressed with ValueUncompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetThreeCompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetThreeCompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetThreeCompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetThreeCompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetThreeCompressed.ValueLengthUncompressed
          }
          object ValueOffsetThreeCompressed extends ValueOffsetThreeCompressed {
            trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetThreeCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthOneCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthOneCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthOneCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthOneCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthOneCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthOneCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthOneCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthOneCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthOneCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthOneCompressed.DeadlineUncompressed
            }

            object ValueLengthOneCompressed extends ValueLengthOneCompressed {
              object NoDeadline extends BaseEntryId(100) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryId(101) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryId(102) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryId(103) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryId(104) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryId(105) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryId(106) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryId(107) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryId(108) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryId(109) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetThreeCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthTwoCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthTwoCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthTwoCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthTwoCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthTwoCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthTwoCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthTwoCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthTwoCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthTwoCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthTwoCompressed.DeadlineUncompressed
            }

            object ValueLengthTwoCompressed extends ValueLengthTwoCompressed {
              object NoDeadline extends BaseEntryId(110) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryId(111) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryId(112) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryId(113) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryId(114) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryId(115) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryId(116) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryId(117) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryId(118) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryId(119) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetThreeCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthThreeCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthThreeCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthThreeCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthThreeCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthThreeCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthThreeCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthThreeCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthThreeCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthThreeCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthThreeCompressed.DeadlineUncompressed
            }

            object ValueLengthThreeCompressed extends ValueLengthThreeCompressed {
              object NoDeadline extends BaseEntryId(120) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryId(121) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryId(122) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryId(123) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryId(124) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryId(125) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryId(126) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryId(127) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryId(128) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryId(129) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetThreeCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthFullyCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthFullyCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthFullyCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthFullyCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthFullyCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthFullyCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthFullyCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthFullyCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthFullyCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthFullyCompressed.DeadlineUncompressed
            }

            object ValueLengthFullyCompressed extends ValueLengthFullyCompressed {
              object NoDeadline extends BaseEntryId(130) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryId(131) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryId(132) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryId(133) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryId(134) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryId(135) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryId(136) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryId(137) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryId(138) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryId(139) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetThreeCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthUncompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthUncompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthUncompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthUncompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthUncompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthUncompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthUncompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthUncompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthUncompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthUncompressed.DeadlineUncompressed
            }

            object ValueLengthUncompressed extends ValueLengthUncompressed {
              object NoDeadline extends BaseEntryId(140) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryId(141) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryId(142) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryId(143) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryId(144) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryId(145) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryId(146) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryId(147) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryId(148) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryId(149) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }

          trait ValueOffsetUncompressed extends ValueOffset.Uncompressed with ValueUncompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetUncompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetUncompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetUncompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetUncompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetUncompressed.ValueLengthUncompressed
          }
          object ValueOffsetUncompressed extends ValueOffsetUncompressed {
            trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetUncompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthOneCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthOneCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthOneCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthOneCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthOneCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthOneCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthOneCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthOneCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthOneCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthOneCompressed.DeadlineUncompressed
            }

            object ValueLengthOneCompressed extends ValueLengthOneCompressed {
              object NoDeadline extends BaseEntryId(150) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryId(151) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryId(152) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryId(153) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryId(154) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryId(155) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryId(156) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryId(157) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryId(158) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryId(159) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetUncompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthTwoCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthTwoCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthTwoCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthTwoCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthTwoCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthTwoCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthTwoCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthTwoCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthTwoCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthTwoCompressed.DeadlineUncompressed
            }

            object ValueLengthTwoCompressed extends ValueLengthTwoCompressed {
              object NoDeadline extends BaseEntryId(160) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryId(161) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryId(162) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryId(163) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryId(164) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryId(165) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryId(166) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryId(167) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryId(168) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryId(169) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetUncompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthThreeCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthThreeCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthThreeCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthThreeCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthThreeCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthThreeCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthThreeCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthThreeCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthThreeCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthThreeCompressed.DeadlineUncompressed
            }

            object ValueLengthThreeCompressed extends ValueLengthThreeCompressed {
              object NoDeadline extends BaseEntryId(170) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryId(171) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryId(172) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryId(173) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryId(174) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryId(175) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryId(176) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryId(177) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryId(178) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryId(179) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetUncompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthFullyCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthFullyCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthFullyCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthFullyCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthFullyCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthFullyCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthFullyCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthFullyCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthFullyCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthFullyCompressed.DeadlineUncompressed
            }

            object ValueLengthFullyCompressed extends ValueLengthFullyCompressed {
              object NoDeadline extends BaseEntryId(180) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryId(181) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryId(182) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryId(183) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryId(184) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryId(185) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryId(186) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryId(187) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryId(188) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryId(189) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetUncompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthUncompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthUncompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthUncompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthUncompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthUncompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthUncompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthUncompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthUncompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthUncompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthUncompressed.DeadlineUncompressed
            }

            object ValueLengthUncompressed extends ValueLengthUncompressed {
              object NoDeadline extends BaseEntryId(190) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryId(191) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryId(192) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryId(193) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryId(194) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryId(195) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryId(196) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryId(197) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryId(198) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryId(199) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }
        }

        trait NoValue extends Value.NoValue with TimePartiallyCompressed {
          override def noDeadline: Deadline.NoDeadline = NoValue.NoDeadline
          override def deadlineOneCompressed: Deadline.OneCompressed = NoValue.DeadlineOneCompressed
          override def deadlineTwoCompressed: Deadline.TwoCompressed = NoValue.DeadlineTwoCompressed
          override def deadlineThreeCompressed: Deadline.ThreeCompressed = NoValue.DeadlineThreeCompressed
          override def deadlineFourCompressed: Deadline.FourCompressed = NoValue.DeadlineFourCompressed
          override def deadlineFiveCompressed: Deadline.FiveCompressed = NoValue.DeadlineFiveCompressed
          override def deadlineSixCompressed: Deadline.SixCompressed = NoValue.DeadlineSixCompressed
          override def deadlineSevenCompressed: Deadline.SevenCompressed = NoValue.DeadlineSevenCompressed
          override def deadlineFullyCompressed: Deadline.FullyCompressed = NoValue.DeadlineFullyCompressed
          override def deadlineUncompressed: Deadline.Uncompressed = NoValue.DeadlineUncompressed
        }
        object NoValue extends NoValue {
          object NoDeadline extends BaseEntryId(200) with Deadline.NoDeadline with NoValue
          object DeadlineOneCompressed extends BaseEntryId(201) with Deadline.OneCompressed with NoValue
          object DeadlineTwoCompressed extends BaseEntryId(202) with Deadline.TwoCompressed with NoValue
          object DeadlineThreeCompressed extends BaseEntryId(203) with Deadline.ThreeCompressed with NoValue
          object DeadlineFourCompressed extends BaseEntryId(204) with Deadline.FourCompressed with NoValue
          object DeadlineFiveCompressed extends BaseEntryId(205) with Deadline.FiveCompressed with NoValue
          object DeadlineSixCompressed extends BaseEntryId(206) with Deadline.SixCompressed with NoValue
          object DeadlineSevenCompressed extends BaseEntryId(207) with Deadline.SevenCompressed with NoValue
          object DeadlineFullyCompressed extends BaseEntryId(208) with Deadline.FullyCompressed with NoValue
          object DeadlineUncompressed extends BaseEntryId(209) with Deadline.Uncompressed with NoValue
        }

        trait ValueFullyCompressed extends Value.FullyCompressed with TimePartiallyCompressed {
          override def noDeadline: Deadline.NoDeadline = ValueFullyCompressed.NoDeadline
          override def deadlineOneCompressed: Deadline.OneCompressed = ValueFullyCompressed.DeadlineOneCompressed
          override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueFullyCompressed.DeadlineTwoCompressed
          override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueFullyCompressed.DeadlineThreeCompressed
          override def deadlineFourCompressed: Deadline.FourCompressed = ValueFullyCompressed.DeadlineFourCompressed
          override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueFullyCompressed.DeadlineFiveCompressed
          override def deadlineSixCompressed: Deadline.SixCompressed = ValueFullyCompressed.DeadlineSixCompressed
          override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueFullyCompressed.DeadlineSevenCompressed
          override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueFullyCompressed.DeadlineFullyCompressed
          override def deadlineUncompressed: Deadline.Uncompressed = ValueFullyCompressed.DeadlineUncompressed
        }
        object ValueFullyCompressed extends ValueFullyCompressed {
          object NoDeadline extends BaseEntryId(210) with Deadline.NoDeadline with ValueFullyCompressed
          object DeadlineOneCompressed extends BaseEntryId(211) with Deadline.OneCompressed with ValueFullyCompressed
          object DeadlineTwoCompressed extends BaseEntryId(212) with Deadline.TwoCompressed with ValueFullyCompressed
          object DeadlineThreeCompressed extends BaseEntryId(213) with Deadline.ThreeCompressed with ValueFullyCompressed
          object DeadlineFourCompressed extends BaseEntryId(214) with Deadline.FourCompressed with ValueFullyCompressed
          object DeadlineFiveCompressed extends BaseEntryId(215) with Deadline.FiveCompressed with ValueFullyCompressed
          object DeadlineSixCompressed extends BaseEntryId(216) with Deadline.SixCompressed with ValueFullyCompressed
          object DeadlineSevenCompressed extends BaseEntryId(217) with Deadline.SevenCompressed with ValueFullyCompressed
          object DeadlineFullyCompressed extends BaseEntryId(218) with Deadline.FullyCompressed with ValueFullyCompressed
          object DeadlineUncompressed extends BaseEntryId(219) with Deadline.Uncompressed with ValueFullyCompressed
        }
      }

      trait TimeFullyCompressed extends Time.FullyCompressed with KeyPartiallyCompressed {
        override def noValue: Value.NoValue = TimeFullyCompressed.NoValue
        override def valueFullyCompressed: Value.FullyCompressed = TimeFullyCompressed.ValueFullyCompressed
        override def valueUncompressed: Value.Uncompressed = TimeFullyCompressed.ValueUncompressed
      }
      object TimeFullyCompressed extends TimeFullyCompressed {

        trait ValueUncompressed extends Value.Uncompressed with TimeFullyCompressed {
          override def valueOffsetOneCompressed: ValueOffset.OneCompressed = ValueUncompressed.ValueOffsetOneCompressed
          override def valueOffsetTwoCompressed: ValueOffset.TwoCompressed = ValueUncompressed.ValueOffsetTwoCompressed
          override def valueOffsetThreeCompressed: ValueOffset.ThreeCompressed = ValueUncompressed.ValueOffsetThreeCompressed
          override def valueOffsetUncompressed: ValueOffset.Uncompressed = ValueUncompressed.ValueOffsetUncompressed
        }
        object ValueUncompressed extends ValueUncompressed {

          trait ValueOffsetOneCompressed extends ValueOffset.OneCompressed with ValueUncompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetOneCompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetOneCompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetOneCompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetOneCompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetOneCompressed.ValueLengthUncompressed
          }
          object ValueOffsetOneCompressed extends ValueOffsetOneCompressed {
            trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetOneCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthOneCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthOneCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthOneCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthOneCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthOneCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthOneCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthOneCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthOneCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthOneCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthOneCompressed.DeadlineUncompressed
            }

            object ValueLengthOneCompressed extends ValueLengthOneCompressed {
              object NoDeadline extends BaseEntryId(220) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryId(221) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryId(222) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryId(223) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryId(224) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryId(225) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryId(226) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryId(227) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryId(228) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryId(229) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetOneCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthTwoCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthTwoCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthTwoCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthTwoCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthTwoCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthTwoCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthTwoCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthTwoCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthTwoCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthTwoCompressed.DeadlineUncompressed
            }

            object ValueLengthTwoCompressed extends ValueLengthTwoCompressed {
              object NoDeadline extends BaseEntryId(230) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryId(231) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryId(232) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryId(233) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryId(234) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryId(235) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryId(236) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryId(237) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryId(238) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryId(239) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetOneCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthThreeCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthThreeCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthThreeCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthThreeCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthThreeCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthThreeCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthThreeCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthThreeCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthThreeCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthThreeCompressed.DeadlineUncompressed
            }

            object ValueLengthThreeCompressed extends ValueLengthThreeCompressed {
              object NoDeadline extends BaseEntryId(240) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryId(241) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryId(242) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryId(243) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryId(244) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryId(245) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryId(246) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryId(247) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryId(248) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryId(249) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetOneCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthFullyCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthFullyCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthFullyCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthFullyCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthFullyCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthFullyCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthFullyCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthFullyCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthFullyCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthFullyCompressed.DeadlineUncompressed
            }

            object ValueLengthFullyCompressed extends ValueLengthFullyCompressed {
              object NoDeadline extends BaseEntryId(250) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryId(251) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryId(252) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryId(253) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryId(254) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryId(255) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryId(256) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryId(257) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryId(258) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryId(259) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetOneCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthUncompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthUncompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthUncompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthUncompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthUncompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthUncompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthUncompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthUncompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthUncompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthUncompressed.DeadlineUncompressed
            }

            object ValueLengthUncompressed extends ValueLengthUncompressed {
              object NoDeadline extends BaseEntryId(260) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryId(261) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryId(262) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryId(263) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryId(264) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryId(265) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryId(266) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryId(267) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryId(268) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryId(269) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }

          trait ValueOffsetTwoCompressed extends ValueOffset.TwoCompressed with ValueUncompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetTwoCompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetTwoCompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetTwoCompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetTwoCompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetTwoCompressed.ValueLengthUncompressed
          }
          object ValueOffsetTwoCompressed extends ValueOffsetTwoCompressed {
            trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetTwoCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthOneCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthOneCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthOneCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthOneCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthOneCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthOneCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthOneCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthOneCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthOneCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthOneCompressed.DeadlineUncompressed
            }

            object ValueLengthOneCompressed extends ValueLengthOneCompressed {
              object NoDeadline extends BaseEntryId(270) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryId(271) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryId(272) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryId(273) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryId(274) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryId(275) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryId(276) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryId(277) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryId(278) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryId(279) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetTwoCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthTwoCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthTwoCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthTwoCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthTwoCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthTwoCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthTwoCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthTwoCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthTwoCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthTwoCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthTwoCompressed.DeadlineUncompressed
            }

            object ValueLengthTwoCompressed extends ValueLengthTwoCompressed {
              object NoDeadline extends BaseEntryId(280) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryId(281) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryId(282) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryId(283) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryId(284) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryId(285) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryId(286) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryId(287) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryId(288) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryId(289) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetTwoCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthThreeCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthThreeCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthThreeCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthThreeCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthThreeCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthThreeCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthThreeCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthThreeCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthThreeCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthThreeCompressed.DeadlineUncompressed
            }

            object ValueLengthThreeCompressed extends ValueLengthThreeCompressed {
              object NoDeadline extends BaseEntryId(290) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryId(291) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryId(292) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryId(293) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryId(294) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryId(295) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryId(296) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryId(297) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryId(298) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryId(299) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetTwoCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthFullyCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthFullyCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthFullyCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthFullyCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthFullyCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthFullyCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthFullyCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthFullyCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthFullyCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthFullyCompressed.DeadlineUncompressed
            }

            object ValueLengthFullyCompressed extends ValueLengthFullyCompressed {
              object NoDeadline extends BaseEntryId(300) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryId(301) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryId(302) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryId(303) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryId(304) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryId(305) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryId(306) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryId(307) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryId(308) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryId(309) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetTwoCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthUncompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthUncompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthUncompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthUncompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthUncompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthUncompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthUncompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthUncompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthUncompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthUncompressed.DeadlineUncompressed
            }

            object ValueLengthUncompressed extends ValueLengthUncompressed {
              object NoDeadline extends BaseEntryId(310) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryId(311) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryId(312) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryId(313) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryId(314) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryId(315) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryId(316) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryId(317) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryId(318) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryId(319) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }

          trait ValueOffsetThreeCompressed extends ValueOffset.ThreeCompressed with ValueUncompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetThreeCompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetThreeCompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetThreeCompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetThreeCompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetThreeCompressed.ValueLengthUncompressed
          }
          object ValueOffsetThreeCompressed extends ValueOffsetThreeCompressed {
            trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetThreeCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthOneCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthOneCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthOneCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthOneCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthOneCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthOneCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthOneCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthOneCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthOneCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthOneCompressed.DeadlineUncompressed
            }

            object ValueLengthOneCompressed extends ValueLengthOneCompressed {
              object NoDeadline extends BaseEntryId(320) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryId(321) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryId(322) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryId(323) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryId(324) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryId(325) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryId(326) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryId(327) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryId(328) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryId(329) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetThreeCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthTwoCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthTwoCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthTwoCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthTwoCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthTwoCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthTwoCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthTwoCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthTwoCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthTwoCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthTwoCompressed.DeadlineUncompressed
            }

            object ValueLengthTwoCompressed extends ValueLengthTwoCompressed {
              object NoDeadline extends BaseEntryId(330) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryId(331) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryId(332) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryId(333) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryId(334) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryId(335) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryId(336) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryId(337) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryId(338) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryId(339) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetThreeCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthThreeCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthThreeCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthThreeCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthThreeCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthThreeCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthThreeCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthThreeCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthThreeCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthThreeCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthThreeCompressed.DeadlineUncompressed
            }

            object ValueLengthThreeCompressed extends ValueLengthThreeCompressed {
              object NoDeadline extends BaseEntryId(340) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryId(341) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryId(342) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryId(343) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryId(344) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryId(345) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryId(346) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryId(347) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryId(348) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryId(349) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetThreeCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthFullyCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthFullyCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthFullyCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthFullyCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthFullyCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthFullyCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthFullyCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthFullyCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthFullyCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthFullyCompressed.DeadlineUncompressed
            }

            object ValueLengthFullyCompressed extends ValueLengthFullyCompressed {
              object NoDeadline extends BaseEntryId(350) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryId(351) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryId(352) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryId(353) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryId(354) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryId(355) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryId(356) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryId(357) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryId(358) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryId(359) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetThreeCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthUncompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthUncompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthUncompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthUncompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthUncompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthUncompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthUncompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthUncompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthUncompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthUncompressed.DeadlineUncompressed
            }

            object ValueLengthUncompressed extends ValueLengthUncompressed {
              object NoDeadline extends BaseEntryId(360) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryId(361) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryId(362) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryId(363) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryId(364) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryId(365) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryId(366) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryId(367) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryId(368) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryId(369) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }

          trait ValueOffsetUncompressed extends ValueOffset.Uncompressed with ValueUncompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetUncompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetUncompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetUncompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetUncompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetUncompressed.ValueLengthUncompressed
          }
          object ValueOffsetUncompressed extends ValueOffsetUncompressed {
            trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetUncompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthOneCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthOneCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthOneCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthOneCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthOneCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthOneCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthOneCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthOneCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthOneCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthOneCompressed.DeadlineUncompressed
            }

            object ValueLengthOneCompressed extends ValueLengthOneCompressed {
              object NoDeadline extends BaseEntryId(370) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryId(371) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryId(372) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryId(373) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryId(374) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryId(375) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryId(376) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryId(377) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryId(378) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryId(379) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetUncompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthTwoCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthTwoCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthTwoCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthTwoCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthTwoCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthTwoCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthTwoCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthTwoCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthTwoCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthTwoCompressed.DeadlineUncompressed
            }

            object ValueLengthTwoCompressed extends ValueLengthTwoCompressed {
              object NoDeadline extends BaseEntryId(380) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryId(381) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryId(382) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryId(383) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryId(384) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryId(385) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryId(386) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryId(387) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryId(388) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryId(389) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetUncompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthThreeCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthThreeCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthThreeCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthThreeCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthThreeCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthThreeCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthThreeCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthThreeCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthThreeCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthThreeCompressed.DeadlineUncompressed
            }

            object ValueLengthThreeCompressed extends ValueLengthThreeCompressed {
              object NoDeadline extends BaseEntryId(390) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryId(391) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryId(392) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryId(393) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryId(394) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryId(395) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryId(396) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryId(397) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryId(398) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryId(399) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetUncompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthFullyCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthFullyCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthFullyCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthFullyCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthFullyCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthFullyCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthFullyCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthFullyCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthFullyCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthFullyCompressed.DeadlineUncompressed
            }

            object ValueLengthFullyCompressed extends ValueLengthFullyCompressed {
              object NoDeadline extends BaseEntryId(400) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryId(401) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryId(402) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryId(403) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryId(404) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryId(405) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryId(406) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryId(407) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryId(408) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryId(409) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetUncompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthUncompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthUncompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthUncompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthUncompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthUncompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthUncompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthUncompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthUncompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthUncompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthUncompressed.DeadlineUncompressed
            }

            object ValueLengthUncompressed extends ValueLengthUncompressed {
              object NoDeadline extends BaseEntryId(410) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryId(411) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryId(412) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryId(413) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryId(414) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryId(415) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryId(416) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryId(417) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryId(418) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryId(419) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }
        }

        trait NoValue extends Value.NoValue with TimeFullyCompressed {
          override def noDeadline: Deadline.NoDeadline = NoValue.NoDeadline
          override def deadlineOneCompressed: Deadline.OneCompressed = NoValue.DeadlineOneCompressed
          override def deadlineTwoCompressed: Deadline.TwoCompressed = NoValue.DeadlineTwoCompressed
          override def deadlineThreeCompressed: Deadline.ThreeCompressed = NoValue.DeadlineThreeCompressed
          override def deadlineFourCompressed: Deadline.FourCompressed = NoValue.DeadlineFourCompressed
          override def deadlineFiveCompressed: Deadline.FiveCompressed = NoValue.DeadlineFiveCompressed
          override def deadlineSixCompressed: Deadline.SixCompressed = NoValue.DeadlineSixCompressed
          override def deadlineSevenCompressed: Deadline.SevenCompressed = NoValue.DeadlineSevenCompressed
          override def deadlineFullyCompressed: Deadline.FullyCompressed = NoValue.DeadlineFullyCompressed
          override def deadlineUncompressed: Deadline.Uncompressed = NoValue.DeadlineUncompressed
        }
        object NoValue extends NoValue {
          object NoDeadline extends BaseEntryId(420) with Deadline.NoDeadline with NoValue
          object DeadlineOneCompressed extends BaseEntryId(421) with Deadline.OneCompressed with NoValue
          object DeadlineTwoCompressed extends BaseEntryId(422) with Deadline.TwoCompressed with NoValue
          object DeadlineThreeCompressed extends BaseEntryId(423) with Deadline.ThreeCompressed with NoValue
          object DeadlineFourCompressed extends BaseEntryId(424) with Deadline.FourCompressed with NoValue
          object DeadlineFiveCompressed extends BaseEntryId(425) with Deadline.FiveCompressed with NoValue
          object DeadlineSixCompressed extends BaseEntryId(426) with Deadline.SixCompressed with NoValue
          object DeadlineSevenCompressed extends BaseEntryId(427) with Deadline.SevenCompressed with NoValue
          object DeadlineFullyCompressed extends BaseEntryId(428) with Deadline.FullyCompressed with NoValue
          object DeadlineUncompressed extends BaseEntryId(429) with Deadline.Uncompressed with NoValue
        }

        trait ValueFullyCompressed extends Value.FullyCompressed with TimeFullyCompressed {
          override def noDeadline: Deadline.NoDeadline = ValueFullyCompressed.NoDeadline
          override def deadlineOneCompressed: Deadline.OneCompressed = ValueFullyCompressed.DeadlineOneCompressed
          override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueFullyCompressed.DeadlineTwoCompressed
          override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueFullyCompressed.DeadlineThreeCompressed
          override def deadlineFourCompressed: Deadline.FourCompressed = ValueFullyCompressed.DeadlineFourCompressed
          override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueFullyCompressed.DeadlineFiveCompressed
          override def deadlineSixCompressed: Deadline.SixCompressed = ValueFullyCompressed.DeadlineSixCompressed
          override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueFullyCompressed.DeadlineSevenCompressed
          override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueFullyCompressed.DeadlineFullyCompressed
          override def deadlineUncompressed: Deadline.Uncompressed = ValueFullyCompressed.DeadlineUncompressed
        }
        object ValueFullyCompressed extends ValueFullyCompressed {
          object NoDeadline extends BaseEntryId(430) with Deadline.NoDeadline with ValueFullyCompressed
          object DeadlineOneCompressed extends BaseEntryId(431) with Deadline.OneCompressed with ValueFullyCompressed
          object DeadlineTwoCompressed extends BaseEntryId(432) with Deadline.TwoCompressed with ValueFullyCompressed
          object DeadlineThreeCompressed extends BaseEntryId(433) with Deadline.ThreeCompressed with ValueFullyCompressed
          object DeadlineFourCompressed extends BaseEntryId(434) with Deadline.FourCompressed with ValueFullyCompressed
          object DeadlineFiveCompressed extends BaseEntryId(435) with Deadline.FiveCompressed with ValueFullyCompressed
          object DeadlineSixCompressed extends BaseEntryId(436) with Deadline.SixCompressed with ValueFullyCompressed
          object DeadlineSevenCompressed extends BaseEntryId(437) with Deadline.SevenCompressed with ValueFullyCompressed
          object DeadlineFullyCompressed extends BaseEntryId(438) with Deadline.FullyCompressed with ValueFullyCompressed
          object DeadlineUncompressed extends BaseEntryId(439) with Deadline.Uncompressed with ValueFullyCompressed
        }
      }

      trait TimeUncompressed extends Time.Uncompressed with KeyPartiallyCompressed {
        override def noValue: Value.NoValue = TimeUncompressed.NoValue
        override def valueFullyCompressed: Value.FullyCompressed = TimeUncompressed.ValueFullyCompressed
        override def valueUncompressed: Value.Uncompressed = TimeUncompressed.ValueUncompressed
      }
      object TimeUncompressed extends TimeUncompressed {

        trait ValueUncompressed extends Value.Uncompressed with TimeUncompressed {
          override def valueOffsetOneCompressed: ValueOffset.OneCompressed = ValueUncompressed.ValueOffsetOneCompressed
          override def valueOffsetTwoCompressed: ValueOffset.TwoCompressed = ValueUncompressed.ValueOffsetTwoCompressed
          override def valueOffsetThreeCompressed: ValueOffset.ThreeCompressed = ValueUncompressed.ValueOffsetThreeCompressed
          override def valueOffsetUncompressed: ValueOffset.Uncompressed = ValueUncompressed.ValueOffsetUncompressed
        }
        object ValueUncompressed extends ValueUncompressed {

          trait ValueOffsetOneCompressed extends ValueOffset.OneCompressed with ValueUncompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetOneCompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetOneCompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetOneCompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetOneCompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetOneCompressed.ValueLengthUncompressed
          }
          object ValueOffsetOneCompressed extends ValueOffsetOneCompressed {
            trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetOneCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthOneCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthOneCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthOneCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthOneCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthOneCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthOneCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthOneCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthOneCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthOneCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthOneCompressed.DeadlineUncompressed
            }

            object ValueLengthOneCompressed extends ValueLengthOneCompressed {
              object NoDeadline extends BaseEntryId(440) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryId(441) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryId(442) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryId(443) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryId(444) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryId(445) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryId(446) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryId(447) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryId(448) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryId(449) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetOneCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthTwoCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthTwoCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthTwoCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthTwoCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthTwoCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthTwoCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthTwoCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthTwoCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthTwoCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthTwoCompressed.DeadlineUncompressed
            }

            object ValueLengthTwoCompressed extends ValueLengthTwoCompressed {
              object NoDeadline extends BaseEntryId(450) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryId(451) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryId(452) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryId(453) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryId(454) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryId(455) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryId(456) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryId(457) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryId(458) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryId(459) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetOneCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthThreeCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthThreeCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthThreeCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthThreeCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthThreeCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthThreeCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthThreeCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthThreeCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthThreeCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthThreeCompressed.DeadlineUncompressed
            }

            object ValueLengthThreeCompressed extends ValueLengthThreeCompressed {
              object NoDeadline extends BaseEntryId(460) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryId(461) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryId(462) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryId(463) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryId(464) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryId(465) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryId(466) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryId(467) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryId(468) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryId(469) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetOneCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthFullyCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthFullyCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthFullyCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthFullyCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthFullyCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthFullyCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthFullyCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthFullyCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthFullyCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthFullyCompressed.DeadlineUncompressed
            }

            object ValueLengthFullyCompressed extends ValueLengthFullyCompressed {
              object NoDeadline extends BaseEntryId(470) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryId(471) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryId(472) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryId(473) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryId(474) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryId(475) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryId(476) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryId(477) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryId(478) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryId(479) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetOneCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthUncompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthUncompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthUncompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthUncompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthUncompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthUncompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthUncompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthUncompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthUncompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthUncompressed.DeadlineUncompressed
            }

            object ValueLengthUncompressed extends ValueLengthUncompressed {
              object NoDeadline extends BaseEntryId(480) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryId(481) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryId(482) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryId(483) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryId(484) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryId(485) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryId(486) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryId(487) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryId(488) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryId(489) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }

          trait ValueOffsetTwoCompressed extends ValueOffset.TwoCompressed with ValueUncompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetTwoCompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetTwoCompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetTwoCompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetTwoCompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetTwoCompressed.ValueLengthUncompressed
          }
          object ValueOffsetTwoCompressed extends ValueOffsetTwoCompressed {
            trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetTwoCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthOneCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthOneCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthOneCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthOneCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthOneCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthOneCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthOneCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthOneCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthOneCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthOneCompressed.DeadlineUncompressed
            }

            object ValueLengthOneCompressed extends ValueLengthOneCompressed {
              object NoDeadline extends BaseEntryId(490) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryId(491) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryId(492) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryId(493) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryId(494) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryId(495) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryId(496) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryId(497) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryId(498) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryId(499) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetTwoCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthTwoCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthTwoCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthTwoCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthTwoCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthTwoCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthTwoCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthTwoCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthTwoCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthTwoCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthTwoCompressed.DeadlineUncompressed
            }

            object ValueLengthTwoCompressed extends ValueLengthTwoCompressed {
              object NoDeadline extends BaseEntryId(500) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryId(501) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryId(502) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryId(503) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryId(504) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryId(505) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryId(506) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryId(507) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryId(508) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryId(509) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetTwoCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthThreeCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthThreeCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthThreeCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthThreeCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthThreeCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthThreeCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthThreeCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthThreeCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthThreeCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthThreeCompressed.DeadlineUncompressed
            }

            object ValueLengthThreeCompressed extends ValueLengthThreeCompressed {
              object NoDeadline extends BaseEntryId(510) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryId(511) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryId(512) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryId(513) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryId(514) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryId(515) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryId(516) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryId(517) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryId(518) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryId(519) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetTwoCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthFullyCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthFullyCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthFullyCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthFullyCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthFullyCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthFullyCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthFullyCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthFullyCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthFullyCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthFullyCompressed.DeadlineUncompressed
            }

            object ValueLengthFullyCompressed extends ValueLengthFullyCompressed {
              object NoDeadline extends BaseEntryId(520) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryId(521) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryId(522) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryId(523) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryId(524) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryId(525) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryId(526) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryId(527) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryId(528) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryId(529) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetTwoCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthUncompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthUncompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthUncompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthUncompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthUncompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthUncompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthUncompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthUncompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthUncompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthUncompressed.DeadlineUncompressed
            }

            object ValueLengthUncompressed extends ValueLengthUncompressed {
              object NoDeadline extends BaseEntryId(530) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryId(531) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryId(532) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryId(533) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryId(534) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryId(535) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryId(536) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryId(537) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryId(538) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryId(539) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }

          trait ValueOffsetThreeCompressed extends ValueOffset.ThreeCompressed with ValueUncompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetThreeCompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetThreeCompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetThreeCompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetThreeCompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetThreeCompressed.ValueLengthUncompressed
          }
          object ValueOffsetThreeCompressed extends ValueOffsetThreeCompressed {
            trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetThreeCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthOneCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthOneCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthOneCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthOneCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthOneCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthOneCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthOneCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthOneCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthOneCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthOneCompressed.DeadlineUncompressed
            }

            object ValueLengthOneCompressed extends ValueLengthOneCompressed {
              object NoDeadline extends BaseEntryId(540) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryId(541) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryId(542) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryId(543) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryId(544) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryId(545) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryId(546) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryId(547) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryId(548) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryId(549) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetThreeCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthTwoCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthTwoCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthTwoCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthTwoCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthTwoCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthTwoCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthTwoCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthTwoCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthTwoCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthTwoCompressed.DeadlineUncompressed
            }

            object ValueLengthTwoCompressed extends ValueLengthTwoCompressed {
              object NoDeadline extends BaseEntryId(550) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryId(551) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryId(552) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryId(553) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryId(554) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryId(555) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryId(556) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryId(557) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryId(558) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryId(559) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetThreeCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthThreeCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthThreeCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthThreeCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthThreeCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthThreeCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthThreeCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthThreeCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthThreeCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthThreeCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthThreeCompressed.DeadlineUncompressed
            }

            object ValueLengthThreeCompressed extends ValueLengthThreeCompressed {
              object NoDeadline extends BaseEntryId(560) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryId(561) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryId(562) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryId(563) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryId(564) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryId(565) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryId(566) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryId(567) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryId(568) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryId(569) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetThreeCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthFullyCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthFullyCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthFullyCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthFullyCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthFullyCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthFullyCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthFullyCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthFullyCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthFullyCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthFullyCompressed.DeadlineUncompressed
            }

            object ValueLengthFullyCompressed extends ValueLengthFullyCompressed {
              object NoDeadline extends BaseEntryId(570) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryId(571) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryId(572) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryId(573) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryId(574) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryId(575) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryId(576) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryId(577) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryId(578) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryId(579) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetThreeCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthUncompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthUncompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthUncompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthUncompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthUncompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthUncompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthUncompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthUncompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthUncompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthUncompressed.DeadlineUncompressed
            }

            object ValueLengthUncompressed extends ValueLengthUncompressed {
              object NoDeadline extends BaseEntryId(580) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryId(581) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryId(582) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryId(583) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryId(584) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryId(585) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryId(586) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryId(587) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryId(588) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryId(589) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }

          trait ValueOffsetUncompressed extends ValueOffset.Uncompressed with ValueUncompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetUncompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetUncompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetUncompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetUncompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetUncompressed.ValueLengthUncompressed
          }
          object ValueOffsetUncompressed extends ValueOffsetUncompressed {
            trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetUncompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthOneCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthOneCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthOneCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthOneCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthOneCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthOneCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthOneCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthOneCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthOneCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthOneCompressed.DeadlineUncompressed
            }

            object ValueLengthOneCompressed extends ValueLengthOneCompressed {
              object NoDeadline extends BaseEntryId(590) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryId(591) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryId(592) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryId(593) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryId(594) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryId(595) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryId(596) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryId(597) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryId(598) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryId(599) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetUncompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthTwoCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthTwoCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthTwoCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthTwoCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthTwoCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthTwoCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthTwoCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthTwoCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthTwoCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthTwoCompressed.DeadlineUncompressed
            }

            object ValueLengthTwoCompressed extends ValueLengthTwoCompressed {
              object NoDeadline extends BaseEntryId(600) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryId(601) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryId(602) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryId(603) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryId(604) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryId(605) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryId(606) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryId(607) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryId(608) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryId(609) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetUncompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthThreeCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthThreeCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthThreeCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthThreeCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthThreeCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthThreeCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthThreeCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthThreeCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthThreeCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthThreeCompressed.DeadlineUncompressed
            }

            object ValueLengthThreeCompressed extends ValueLengthThreeCompressed {
              object NoDeadline extends BaseEntryId(610) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryId(611) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryId(612) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryId(613) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryId(614) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryId(615) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryId(616) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryId(617) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryId(618) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryId(619) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetUncompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthFullyCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthFullyCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthFullyCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthFullyCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthFullyCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthFullyCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthFullyCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthFullyCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthFullyCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthFullyCompressed.DeadlineUncompressed
            }

            object ValueLengthFullyCompressed extends ValueLengthFullyCompressed {
              object NoDeadline extends BaseEntryId(620) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryId(621) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryId(622) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryId(623) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryId(624) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryId(625) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryId(626) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryId(627) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryId(628) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryId(629) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetUncompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthUncompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthUncompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthUncompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthUncompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthUncompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthUncompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthUncompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthUncompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthUncompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthUncompressed.DeadlineUncompressed
            }

            object ValueLengthUncompressed extends ValueLengthUncompressed {
              object NoDeadline extends BaseEntryId(630) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryId(631) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryId(632) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryId(633) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryId(634) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryId(635) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryId(636) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryId(637) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryId(638) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryId(639) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }
        }

        trait NoValue extends Value.NoValue with TimeUncompressed {
          override def noDeadline: Deadline.NoDeadline = NoValue.NoDeadline
          override def deadlineOneCompressed: Deadline.OneCompressed = NoValue.DeadlineOneCompressed
          override def deadlineTwoCompressed: Deadline.TwoCompressed = NoValue.DeadlineTwoCompressed
          override def deadlineThreeCompressed: Deadline.ThreeCompressed = NoValue.DeadlineThreeCompressed
          override def deadlineFourCompressed: Deadline.FourCompressed = NoValue.DeadlineFourCompressed
          override def deadlineFiveCompressed: Deadline.FiveCompressed = NoValue.DeadlineFiveCompressed
          override def deadlineSixCompressed: Deadline.SixCompressed = NoValue.DeadlineSixCompressed
          override def deadlineSevenCompressed: Deadline.SevenCompressed = NoValue.DeadlineSevenCompressed
          override def deadlineFullyCompressed: Deadline.FullyCompressed = NoValue.DeadlineFullyCompressed
          override def deadlineUncompressed: Deadline.Uncompressed = NoValue.DeadlineUncompressed
        }
        object NoValue extends NoValue {
          object NoDeadline extends BaseEntryId(640) with Deadline.NoDeadline with NoValue
          object DeadlineOneCompressed extends BaseEntryId(641) with Deadline.OneCompressed with NoValue
          object DeadlineTwoCompressed extends BaseEntryId(642) with Deadline.TwoCompressed with NoValue
          object DeadlineThreeCompressed extends BaseEntryId(643) with Deadline.ThreeCompressed with NoValue
          object DeadlineFourCompressed extends BaseEntryId(644) with Deadline.FourCompressed with NoValue
          object DeadlineFiveCompressed extends BaseEntryId(645) with Deadline.FiveCompressed with NoValue
          object DeadlineSixCompressed extends BaseEntryId(646) with Deadline.SixCompressed with NoValue
          object DeadlineSevenCompressed extends BaseEntryId(647) with Deadline.SevenCompressed with NoValue
          object DeadlineFullyCompressed extends BaseEntryId(648) with Deadline.FullyCompressed with NoValue
          object DeadlineUncompressed extends BaseEntryId(649) with Deadline.Uncompressed with NoValue
        }

        trait ValueFullyCompressed extends Value.FullyCompressed with TimeUncompressed {
          override def noDeadline: Deadline.NoDeadline = ValueFullyCompressed.NoDeadline
          override def deadlineOneCompressed: Deadline.OneCompressed = ValueFullyCompressed.DeadlineOneCompressed
          override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueFullyCompressed.DeadlineTwoCompressed
          override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueFullyCompressed.DeadlineThreeCompressed
          override def deadlineFourCompressed: Deadline.FourCompressed = ValueFullyCompressed.DeadlineFourCompressed
          override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueFullyCompressed.DeadlineFiveCompressed
          override def deadlineSixCompressed: Deadline.SixCompressed = ValueFullyCompressed.DeadlineSixCompressed
          override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueFullyCompressed.DeadlineSevenCompressed
          override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueFullyCompressed.DeadlineFullyCompressed
          override def deadlineUncompressed: Deadline.Uncompressed = ValueFullyCompressed.DeadlineUncompressed
        }
        object ValueFullyCompressed extends ValueFullyCompressed {
          object NoDeadline extends BaseEntryId(650) with Deadline.NoDeadline with ValueFullyCompressed
          object DeadlineOneCompressed extends BaseEntryId(651) with Deadline.OneCompressed with ValueFullyCompressed
          object DeadlineTwoCompressed extends BaseEntryId(652) with Deadline.TwoCompressed with ValueFullyCompressed
          object DeadlineThreeCompressed extends BaseEntryId(653) with Deadline.ThreeCompressed with ValueFullyCompressed
          object DeadlineFourCompressed extends BaseEntryId(654) with Deadline.FourCompressed with ValueFullyCompressed
          object DeadlineFiveCompressed extends BaseEntryId(655) with Deadline.FiveCompressed with ValueFullyCompressed
          object DeadlineSixCompressed extends BaseEntryId(656) with Deadline.SixCompressed with ValueFullyCompressed
          object DeadlineSevenCompressed extends BaseEntryId(657) with Deadline.SevenCompressed with ValueFullyCompressed
          object DeadlineFullyCompressed extends BaseEntryId(658) with Deadline.FullyCompressed with ValueFullyCompressed
          object DeadlineUncompressed extends BaseEntryId(659) with Deadline.Uncompressed with ValueFullyCompressed
        }
      }

      trait NoTime extends Time.NoTime with KeyPartiallyCompressed {
        override def noValue: Value.NoValue = NoTime.NoValue
        override def valueFullyCompressed: Value.FullyCompressed = NoTime.ValueFullyCompressed
        override def valueUncompressed: Value.Uncompressed = NoTime.ValueUncompressed
      }
      object NoTime extends NoTime {

        trait ValueUncompressed extends Value.Uncompressed with NoTime {
          override def valueOffsetOneCompressed: ValueOffset.OneCompressed = ValueUncompressed.ValueOffsetOneCompressed
          override def valueOffsetTwoCompressed: ValueOffset.TwoCompressed = ValueUncompressed.ValueOffsetTwoCompressed
          override def valueOffsetThreeCompressed: ValueOffset.ThreeCompressed = ValueUncompressed.ValueOffsetThreeCompressed
          override def valueOffsetUncompressed: ValueOffset.Uncompressed = ValueUncompressed.ValueOffsetUncompressed
        }
        object ValueUncompressed extends ValueUncompressed {

          trait ValueOffsetOneCompressed extends ValueOffset.OneCompressed with ValueUncompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetOneCompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetOneCompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetOneCompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetOneCompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetOneCompressed.ValueLengthUncompressed
          }
          object ValueOffsetOneCompressed extends ValueOffsetOneCompressed {
            trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetOneCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthOneCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthOneCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthOneCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthOneCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthOneCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthOneCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthOneCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthOneCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthOneCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthOneCompressed.DeadlineUncompressed
            }

            object ValueLengthOneCompressed extends ValueLengthOneCompressed {
              object NoDeadline extends BaseEntryId(660) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryId(661) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryId(662) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryId(663) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryId(664) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryId(665) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryId(666) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryId(667) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryId(668) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryId(669) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetOneCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthTwoCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthTwoCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthTwoCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthTwoCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthTwoCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthTwoCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthTwoCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthTwoCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthTwoCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthTwoCompressed.DeadlineUncompressed
            }

            object ValueLengthTwoCompressed extends ValueLengthTwoCompressed {
              object NoDeadline extends BaseEntryId(670) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryId(671) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryId(672) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryId(673) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryId(674) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryId(675) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryId(676) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryId(677) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryId(678) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryId(679) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetOneCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthThreeCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthThreeCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthThreeCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthThreeCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthThreeCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthThreeCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthThreeCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthThreeCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthThreeCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthThreeCompressed.DeadlineUncompressed
            }

            object ValueLengthThreeCompressed extends ValueLengthThreeCompressed {
              object NoDeadline extends BaseEntryId(680) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryId(681) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryId(682) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryId(683) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryId(684) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryId(685) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryId(686) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryId(687) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryId(688) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryId(689) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetOneCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthFullyCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthFullyCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthFullyCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthFullyCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthFullyCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthFullyCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthFullyCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthFullyCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthFullyCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthFullyCompressed.DeadlineUncompressed
            }

            object ValueLengthFullyCompressed extends ValueLengthFullyCompressed {
              object NoDeadline extends BaseEntryId(690) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryId(691) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryId(692) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryId(693) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryId(694) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryId(695) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryId(696) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryId(697) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryId(698) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryId(699) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetOneCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthUncompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthUncompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthUncompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthUncompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthUncompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthUncompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthUncompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthUncompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthUncompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthUncompressed.DeadlineUncompressed
            }

            object ValueLengthUncompressed extends ValueLengthUncompressed {
              object NoDeadline extends BaseEntryId(700) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryId(701) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryId(702) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryId(703) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryId(704) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryId(705) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryId(706) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryId(707) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryId(708) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryId(709) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }

          trait ValueOffsetTwoCompressed extends ValueOffset.TwoCompressed with ValueUncompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetTwoCompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetTwoCompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetTwoCompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetTwoCompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetTwoCompressed.ValueLengthUncompressed
          }
          object ValueOffsetTwoCompressed extends ValueOffsetTwoCompressed {
            trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetTwoCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthOneCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthOneCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthOneCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthOneCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthOneCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthOneCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthOneCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthOneCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthOneCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthOneCompressed.DeadlineUncompressed
            }

            object ValueLengthOneCompressed extends ValueLengthOneCompressed {
              object NoDeadline extends BaseEntryId(710) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryId(711) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryId(712) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryId(713) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryId(714) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryId(715) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryId(716) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryId(717) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryId(718) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryId(719) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetTwoCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthTwoCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthTwoCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthTwoCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthTwoCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthTwoCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthTwoCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthTwoCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthTwoCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthTwoCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthTwoCompressed.DeadlineUncompressed
            }

            object ValueLengthTwoCompressed extends ValueLengthTwoCompressed {
              object NoDeadline extends BaseEntryId(720) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryId(721) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryId(722) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryId(723) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryId(724) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryId(725) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryId(726) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryId(727) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryId(728) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryId(729) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetTwoCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthThreeCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthThreeCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthThreeCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthThreeCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthThreeCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthThreeCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthThreeCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthThreeCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthThreeCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthThreeCompressed.DeadlineUncompressed
            }

            object ValueLengthThreeCompressed extends ValueLengthThreeCompressed {
              object NoDeadline extends BaseEntryId(730) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryId(731) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryId(732) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryId(733) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryId(734) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryId(735) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryId(736) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryId(737) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryId(738) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryId(739) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetTwoCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthFullyCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthFullyCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthFullyCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthFullyCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthFullyCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthFullyCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthFullyCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthFullyCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthFullyCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthFullyCompressed.DeadlineUncompressed
            }

            object ValueLengthFullyCompressed extends ValueLengthFullyCompressed {
              object NoDeadline extends BaseEntryId(740) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryId(741) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryId(742) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryId(743) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryId(744) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryId(745) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryId(746) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryId(747) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryId(748) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryId(749) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetTwoCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthUncompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthUncompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthUncompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthUncompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthUncompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthUncompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthUncompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthUncompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthUncompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthUncompressed.DeadlineUncompressed
            }

            object ValueLengthUncompressed extends ValueLengthUncompressed {
              object NoDeadline extends BaseEntryId(750) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryId(751) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryId(752) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryId(753) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryId(754) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryId(755) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryId(756) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryId(757) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryId(758) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryId(759) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }

          trait ValueOffsetThreeCompressed extends ValueOffset.ThreeCompressed with ValueUncompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetThreeCompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetThreeCompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetThreeCompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetThreeCompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetThreeCompressed.ValueLengthUncompressed
          }
          object ValueOffsetThreeCompressed extends ValueOffsetThreeCompressed {
            trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetThreeCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthOneCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthOneCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthOneCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthOneCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthOneCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthOneCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthOneCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthOneCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthOneCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthOneCompressed.DeadlineUncompressed
            }

            object ValueLengthOneCompressed extends ValueLengthOneCompressed {
              object NoDeadline extends BaseEntryId(760) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryId(761) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryId(762) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryId(763) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryId(764) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryId(765) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryId(766) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryId(767) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryId(768) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryId(769) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetThreeCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthTwoCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthTwoCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthTwoCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthTwoCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthTwoCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthTwoCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthTwoCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthTwoCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthTwoCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthTwoCompressed.DeadlineUncompressed
            }

            object ValueLengthTwoCompressed extends ValueLengthTwoCompressed {
              object NoDeadline extends BaseEntryId(770) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryId(771) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryId(772) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryId(773) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryId(774) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryId(775) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryId(776) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryId(777) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryId(778) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryId(779) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetThreeCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthThreeCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthThreeCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthThreeCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthThreeCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthThreeCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthThreeCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthThreeCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthThreeCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthThreeCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthThreeCompressed.DeadlineUncompressed
            }

            object ValueLengthThreeCompressed extends ValueLengthThreeCompressed {
              object NoDeadline extends BaseEntryId(780) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryId(781) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryId(782) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryId(783) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryId(784) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryId(785) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryId(786) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryId(787) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryId(788) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryId(789) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetThreeCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthFullyCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthFullyCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthFullyCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthFullyCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthFullyCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthFullyCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthFullyCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthFullyCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthFullyCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthFullyCompressed.DeadlineUncompressed
            }

            object ValueLengthFullyCompressed extends ValueLengthFullyCompressed {
              object NoDeadline extends BaseEntryId(790) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryId(791) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryId(792) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryId(793) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryId(794) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryId(795) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryId(796) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryId(797) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryId(798) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryId(799) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetThreeCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthUncompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthUncompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthUncompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthUncompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthUncompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthUncompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthUncompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthUncompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthUncompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthUncompressed.DeadlineUncompressed
            }

            object ValueLengthUncompressed extends ValueLengthUncompressed {
              object NoDeadline extends BaseEntryId(800) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryId(801) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryId(802) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryId(803) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryId(804) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryId(805) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryId(806) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryId(807) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryId(808) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryId(809) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }

          trait ValueOffsetUncompressed extends ValueOffset.Uncompressed with ValueUncompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetUncompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetUncompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetUncompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetUncompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetUncompressed.ValueLengthUncompressed
          }
          object ValueOffsetUncompressed extends ValueOffsetUncompressed {
            trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetUncompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthOneCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthOneCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthOneCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthOneCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthOneCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthOneCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthOneCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthOneCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthOneCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthOneCompressed.DeadlineUncompressed
            }

            object ValueLengthOneCompressed extends ValueLengthOneCompressed {
              object NoDeadline extends BaseEntryId(810) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryId(811) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryId(812) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryId(813) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryId(814) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryId(815) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryId(816) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryId(817) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryId(818) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryId(819) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetUncompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthTwoCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthTwoCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthTwoCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthTwoCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthTwoCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthTwoCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthTwoCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthTwoCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthTwoCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthTwoCompressed.DeadlineUncompressed
            }

            object ValueLengthTwoCompressed extends ValueLengthTwoCompressed {
              object NoDeadline extends BaseEntryId(820) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryId(821) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryId(822) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryId(823) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryId(824) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryId(825) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryId(826) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryId(827) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryId(828) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryId(829) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetUncompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthThreeCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthThreeCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthThreeCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthThreeCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthThreeCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthThreeCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthThreeCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthThreeCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthThreeCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthThreeCompressed.DeadlineUncompressed
            }

            object ValueLengthThreeCompressed extends ValueLengthThreeCompressed {
              object NoDeadline extends BaseEntryId(830) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryId(831) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryId(832) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryId(833) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryId(834) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryId(835) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryId(836) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryId(837) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryId(838) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryId(839) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetUncompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthFullyCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthFullyCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthFullyCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthFullyCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthFullyCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthFullyCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthFullyCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthFullyCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthFullyCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthFullyCompressed.DeadlineUncompressed
            }

            object ValueLengthFullyCompressed extends ValueLengthFullyCompressed {
              object NoDeadline extends BaseEntryId(840) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryId(841) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryId(842) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryId(843) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryId(844) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryId(845) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryId(846) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryId(847) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryId(848) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryId(849) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetUncompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthUncompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthUncompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthUncompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthUncompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthUncompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthUncompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthUncompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthUncompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthUncompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthUncompressed.DeadlineUncompressed
            }

            object ValueLengthUncompressed extends ValueLengthUncompressed {
              object NoDeadline extends BaseEntryId(850) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryId(851) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryId(852) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryId(853) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryId(854) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryId(855) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryId(856) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryId(857) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryId(858) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryId(859) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }
        }

        trait NoValue extends Value.NoValue with NoTime {
          override def noDeadline: Deadline.NoDeadline = NoValue.NoDeadline
          override def deadlineOneCompressed: Deadline.OneCompressed = NoValue.DeadlineOneCompressed
          override def deadlineTwoCompressed: Deadline.TwoCompressed = NoValue.DeadlineTwoCompressed
          override def deadlineThreeCompressed: Deadline.ThreeCompressed = NoValue.DeadlineThreeCompressed
          override def deadlineFourCompressed: Deadline.FourCompressed = NoValue.DeadlineFourCompressed
          override def deadlineFiveCompressed: Deadline.FiveCompressed = NoValue.DeadlineFiveCompressed
          override def deadlineSixCompressed: Deadline.SixCompressed = NoValue.DeadlineSixCompressed
          override def deadlineSevenCompressed: Deadline.SevenCompressed = NoValue.DeadlineSevenCompressed
          override def deadlineFullyCompressed: Deadline.FullyCompressed = NoValue.DeadlineFullyCompressed
          override def deadlineUncompressed: Deadline.Uncompressed = NoValue.DeadlineUncompressed
        }
        object NoValue extends NoValue {
          object NoDeadline extends BaseEntryId(860) with Deadline.NoDeadline with NoValue
          object DeadlineOneCompressed extends BaseEntryId(861) with Deadline.OneCompressed with NoValue
          object DeadlineTwoCompressed extends BaseEntryId(862) with Deadline.TwoCompressed with NoValue
          object DeadlineThreeCompressed extends BaseEntryId(863) with Deadline.ThreeCompressed with NoValue
          object DeadlineFourCompressed extends BaseEntryId(864) with Deadline.FourCompressed with NoValue
          object DeadlineFiveCompressed extends BaseEntryId(865) with Deadline.FiveCompressed with NoValue
          object DeadlineSixCompressed extends BaseEntryId(866) with Deadline.SixCompressed with NoValue
          object DeadlineSevenCompressed extends BaseEntryId(867) with Deadline.SevenCompressed with NoValue
          object DeadlineFullyCompressed extends BaseEntryId(868) with Deadline.FullyCompressed with NoValue
          object DeadlineUncompressed extends BaseEntryId(869) with Deadline.Uncompressed with NoValue
        }

        trait ValueFullyCompressed extends Value.FullyCompressed with NoTime {
          override def noDeadline: Deadline.NoDeadline = ValueFullyCompressed.NoDeadline
          override def deadlineOneCompressed: Deadline.OneCompressed = ValueFullyCompressed.DeadlineOneCompressed
          override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueFullyCompressed.DeadlineTwoCompressed
          override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueFullyCompressed.DeadlineThreeCompressed
          override def deadlineFourCompressed: Deadline.FourCompressed = ValueFullyCompressed.DeadlineFourCompressed
          override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueFullyCompressed.DeadlineFiveCompressed
          override def deadlineSixCompressed: Deadline.SixCompressed = ValueFullyCompressed.DeadlineSixCompressed
          override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueFullyCompressed.DeadlineSevenCompressed
          override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueFullyCompressed.DeadlineFullyCompressed
          override def deadlineUncompressed: Deadline.Uncompressed = ValueFullyCompressed.DeadlineUncompressed
        }
        object ValueFullyCompressed extends ValueFullyCompressed {
          object NoDeadline extends BaseEntryId(870) with Deadline.NoDeadline with ValueFullyCompressed
          object DeadlineOneCompressed extends BaseEntryId(871) with Deadline.OneCompressed with ValueFullyCompressed
          object DeadlineTwoCompressed extends BaseEntryId(872) with Deadline.TwoCompressed with ValueFullyCompressed
          object DeadlineThreeCompressed extends BaseEntryId(873) with Deadline.ThreeCompressed with ValueFullyCompressed
          object DeadlineFourCompressed extends BaseEntryId(874) with Deadline.FourCompressed with ValueFullyCompressed
          object DeadlineFiveCompressed extends BaseEntryId(875) with Deadline.FiveCompressed with ValueFullyCompressed
          object DeadlineSixCompressed extends BaseEntryId(876) with Deadline.SixCompressed with ValueFullyCompressed
          object DeadlineSevenCompressed extends BaseEntryId(877) with Deadline.SevenCompressed with ValueFullyCompressed
          object DeadlineFullyCompressed extends BaseEntryId(878) with Deadline.FullyCompressed with ValueFullyCompressed
          object DeadlineUncompressed extends BaseEntryId(879) with Deadline.Uncompressed with ValueFullyCompressed
        }
      }
    }

    trait KeyFullyCompressed extends Key.FullyCompressed with FormatA {
      override def noTime: Time.NoTime = KeyFullyCompressed.NoTime
      override def timeFullyCompressed: Time.FullyCompressed = KeyFullyCompressed.TimeFullyCompressed
      override def timePartiallyCompressed: Time.PartiallyCompressed = KeyFullyCompressed.TimePartiallyCompressed
      override def timeUncompressed: Time.Uncompressed = KeyFullyCompressed.TimeUncompressed
    }
    object KeyFullyCompressed extends KeyFullyCompressed {

      trait TimePartiallyCompressed extends Time.PartiallyCompressed with KeyFullyCompressed {
        override def noValue: Value.NoValue = TimePartiallyCompressed.NoValue
        override def valueFullyCompressed: Value.FullyCompressed = TimePartiallyCompressed.ValueFullyCompressed
        override def valueUncompressed: Value.Uncompressed = TimePartiallyCompressed.ValueUncompressed
      }
      object TimePartiallyCompressed extends TimePartiallyCompressed {

        trait ValueUncompressed extends Value.Uncompressed with TimePartiallyCompressed {
          override def valueOffsetOneCompressed: ValueOffset.OneCompressed = ValueUncompressed.ValueOffsetOneCompressed
          override def valueOffsetTwoCompressed: ValueOffset.TwoCompressed = ValueUncompressed.ValueOffsetTwoCompressed
          override def valueOffsetThreeCompressed: ValueOffset.ThreeCompressed = ValueUncompressed.ValueOffsetThreeCompressed
          override def valueOffsetUncompressed: ValueOffset.Uncompressed = ValueUncompressed.ValueOffsetUncompressed
        }
        object ValueUncompressed extends ValueUncompressed {

          trait ValueOffsetOneCompressed extends ValueOffset.OneCompressed with ValueUncompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetOneCompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetOneCompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetOneCompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetOneCompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetOneCompressed.ValueLengthUncompressed
          }
          object ValueOffsetOneCompressed extends ValueOffsetOneCompressed {
            trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetOneCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthOneCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthOneCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthOneCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthOneCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthOneCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthOneCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthOneCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthOneCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthOneCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthOneCompressed.DeadlineUncompressed
            }

            object ValueLengthOneCompressed extends ValueLengthOneCompressed {
              object NoDeadline extends BaseEntryId(880) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryId(881) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryId(882) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryId(883) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryId(884) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryId(885) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryId(886) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryId(887) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryId(888) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryId(889) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetOneCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthTwoCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthTwoCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthTwoCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthTwoCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthTwoCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthTwoCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthTwoCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthTwoCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthTwoCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthTwoCompressed.DeadlineUncompressed
            }

            object ValueLengthTwoCompressed extends ValueLengthTwoCompressed {
              object NoDeadline extends BaseEntryId(890) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryId(891) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryId(892) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryId(893) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryId(894) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryId(895) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryId(896) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryId(897) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryId(898) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryId(899) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetOneCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthThreeCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthThreeCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthThreeCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthThreeCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthThreeCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthThreeCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthThreeCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthThreeCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthThreeCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthThreeCompressed.DeadlineUncompressed
            }

            object ValueLengthThreeCompressed extends ValueLengthThreeCompressed {
              object NoDeadline extends BaseEntryId(900) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryId(901) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryId(902) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryId(903) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryId(904) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryId(905) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryId(906) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryId(907) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryId(908) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryId(909) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetOneCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthFullyCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthFullyCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthFullyCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthFullyCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthFullyCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthFullyCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthFullyCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthFullyCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthFullyCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthFullyCompressed.DeadlineUncompressed
            }

            object ValueLengthFullyCompressed extends ValueLengthFullyCompressed {
              object NoDeadline extends BaseEntryId(910) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryId(911) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryId(912) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryId(913) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryId(914) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryId(915) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryId(916) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryId(917) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryId(918) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryId(919) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetOneCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthUncompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthUncompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthUncompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthUncompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthUncompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthUncompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthUncompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthUncompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthUncompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthUncompressed.DeadlineUncompressed
            }

            object ValueLengthUncompressed extends ValueLengthUncompressed {
              object NoDeadline extends BaseEntryId(920) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryId(921) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryId(922) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryId(923) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryId(924) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryId(925) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryId(926) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryId(927) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryId(928) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryId(929) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }

          trait ValueOffsetTwoCompressed extends ValueOffset.TwoCompressed with ValueUncompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetTwoCompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetTwoCompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetTwoCompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetTwoCompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetTwoCompressed.ValueLengthUncompressed
          }
          object ValueOffsetTwoCompressed extends ValueOffsetTwoCompressed {
            trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetTwoCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthOneCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthOneCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthOneCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthOneCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthOneCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthOneCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthOneCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthOneCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthOneCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthOneCompressed.DeadlineUncompressed
            }

            object ValueLengthOneCompressed extends ValueLengthOneCompressed {
              object NoDeadline extends BaseEntryId(930) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryId(931) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryId(932) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryId(933) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryId(934) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryId(935) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryId(936) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryId(937) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryId(938) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryId(939) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetTwoCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthTwoCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthTwoCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthTwoCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthTwoCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthTwoCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthTwoCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthTwoCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthTwoCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthTwoCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthTwoCompressed.DeadlineUncompressed
            }

            object ValueLengthTwoCompressed extends ValueLengthTwoCompressed {
              object NoDeadline extends BaseEntryId(940) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryId(941) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryId(942) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryId(943) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryId(944) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryId(945) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryId(946) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryId(947) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryId(948) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryId(949) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetTwoCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthThreeCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthThreeCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthThreeCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthThreeCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthThreeCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthThreeCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthThreeCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthThreeCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthThreeCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthThreeCompressed.DeadlineUncompressed
            }

            object ValueLengthThreeCompressed extends ValueLengthThreeCompressed {
              object NoDeadline extends BaseEntryId(950) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryId(951) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryId(952) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryId(953) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryId(954) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryId(955) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryId(956) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryId(957) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryId(958) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryId(959) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetTwoCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthFullyCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthFullyCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthFullyCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthFullyCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthFullyCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthFullyCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthFullyCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthFullyCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthFullyCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthFullyCompressed.DeadlineUncompressed
            }

            object ValueLengthFullyCompressed extends ValueLengthFullyCompressed {
              object NoDeadline extends BaseEntryId(960) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryId(961) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryId(962) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryId(963) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryId(964) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryId(965) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryId(966) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryId(967) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryId(968) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryId(969) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetTwoCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthUncompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthUncompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthUncompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthUncompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthUncompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthUncompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthUncompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthUncompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthUncompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthUncompressed.DeadlineUncompressed
            }

            object ValueLengthUncompressed extends ValueLengthUncompressed {
              object NoDeadline extends BaseEntryId(970) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryId(971) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryId(972) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryId(973) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryId(974) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryId(975) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryId(976) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryId(977) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryId(978) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryId(979) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }

          trait ValueOffsetThreeCompressed extends ValueOffset.ThreeCompressed with ValueUncompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetThreeCompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetThreeCompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetThreeCompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetThreeCompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetThreeCompressed.ValueLengthUncompressed
          }
          object ValueOffsetThreeCompressed extends ValueOffsetThreeCompressed {
            trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetThreeCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthOneCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthOneCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthOneCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthOneCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthOneCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthOneCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthOneCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthOneCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthOneCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthOneCompressed.DeadlineUncompressed
            }

            object ValueLengthOneCompressed extends ValueLengthOneCompressed {
              object NoDeadline extends BaseEntryId(980) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryId(981) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryId(982) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryId(983) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryId(984) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryId(985) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryId(986) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryId(987) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryId(988) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryId(989) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetThreeCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthTwoCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthTwoCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthTwoCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthTwoCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthTwoCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthTwoCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthTwoCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthTwoCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthTwoCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthTwoCompressed.DeadlineUncompressed
            }

            object ValueLengthTwoCompressed extends ValueLengthTwoCompressed {
              object NoDeadline extends BaseEntryId(990) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryId(991) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryId(992) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryId(993) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryId(994) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryId(995) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryId(996) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryId(997) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryId(998) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryId(999) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetThreeCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthThreeCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthThreeCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthThreeCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthThreeCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthThreeCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthThreeCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthThreeCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthThreeCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthThreeCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthThreeCompressed.DeadlineUncompressed
            }

            object ValueLengthThreeCompressed extends ValueLengthThreeCompressed {
              object NoDeadline extends BaseEntryId(1000) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryId(1001) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1002) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1003) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryId(1004) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1005) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryId(1006) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1007) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1008) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryId(1009) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetThreeCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthFullyCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthFullyCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthFullyCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthFullyCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthFullyCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthFullyCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthFullyCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthFullyCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthFullyCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthFullyCompressed.DeadlineUncompressed
            }

            object ValueLengthFullyCompressed extends ValueLengthFullyCompressed {
              object NoDeadline extends BaseEntryId(1010) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryId(1011) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1012) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1013) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryId(1014) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1015) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryId(1016) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1017) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1018) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryId(1019) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetThreeCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthUncompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthUncompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthUncompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthUncompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthUncompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthUncompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthUncompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthUncompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthUncompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthUncompressed.DeadlineUncompressed
            }

            object ValueLengthUncompressed extends ValueLengthUncompressed {
              object NoDeadline extends BaseEntryId(1020) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryId(1021) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryId(1022) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryId(1023) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryId(1024) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryId(1025) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryId(1026) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryId(1027) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryId(1028) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryId(1029) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }

          trait ValueOffsetUncompressed extends ValueOffset.Uncompressed with ValueUncompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetUncompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetUncompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetUncompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetUncompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetUncompressed.ValueLengthUncompressed
          }
          object ValueOffsetUncompressed extends ValueOffsetUncompressed {
            trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetUncompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthOneCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthOneCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthOneCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthOneCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthOneCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthOneCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthOneCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthOneCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthOneCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthOneCompressed.DeadlineUncompressed
            }

            object ValueLengthOneCompressed extends ValueLengthOneCompressed {
              object NoDeadline extends BaseEntryId(1030) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryId(1031) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1032) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1033) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryId(1034) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1035) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryId(1036) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1037) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1038) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryId(1039) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetUncompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthTwoCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthTwoCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthTwoCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthTwoCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthTwoCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthTwoCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthTwoCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthTwoCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthTwoCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthTwoCompressed.DeadlineUncompressed
            }

            object ValueLengthTwoCompressed extends ValueLengthTwoCompressed {
              object NoDeadline extends BaseEntryId(1040) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryId(1041) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1042) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1043) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryId(1044) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1045) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryId(1046) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1047) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1048) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryId(1049) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetUncompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthThreeCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthThreeCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthThreeCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthThreeCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthThreeCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthThreeCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthThreeCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthThreeCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthThreeCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthThreeCompressed.DeadlineUncompressed
            }

            object ValueLengthThreeCompressed extends ValueLengthThreeCompressed {
              object NoDeadline extends BaseEntryId(1050) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryId(1051) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1052) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1053) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryId(1054) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1055) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryId(1056) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1057) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1058) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryId(1059) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetUncompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthFullyCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthFullyCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthFullyCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthFullyCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthFullyCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthFullyCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthFullyCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthFullyCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthFullyCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthFullyCompressed.DeadlineUncompressed
            }

            object ValueLengthFullyCompressed extends ValueLengthFullyCompressed {
              object NoDeadline extends BaseEntryId(1060) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryId(1061) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1062) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1063) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryId(1064) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1065) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryId(1066) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1067) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1068) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryId(1069) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetUncompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthUncompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthUncompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthUncompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthUncompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthUncompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthUncompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthUncompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthUncompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthUncompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthUncompressed.DeadlineUncompressed
            }

            object ValueLengthUncompressed extends ValueLengthUncompressed {
              object NoDeadline extends BaseEntryId(1070) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryId(1071) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryId(1072) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryId(1073) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryId(1074) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryId(1075) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryId(1076) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryId(1077) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryId(1078) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryId(1079) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }
        }

        trait NoValue extends Value.NoValue with TimePartiallyCompressed {
          override def noDeadline: Deadline.NoDeadline = NoValue.NoDeadline
          override def deadlineOneCompressed: Deadline.OneCompressed = NoValue.DeadlineOneCompressed
          override def deadlineTwoCompressed: Deadline.TwoCompressed = NoValue.DeadlineTwoCompressed
          override def deadlineThreeCompressed: Deadline.ThreeCompressed = NoValue.DeadlineThreeCompressed
          override def deadlineFourCompressed: Deadline.FourCompressed = NoValue.DeadlineFourCompressed
          override def deadlineFiveCompressed: Deadline.FiveCompressed = NoValue.DeadlineFiveCompressed
          override def deadlineSixCompressed: Deadline.SixCompressed = NoValue.DeadlineSixCompressed
          override def deadlineSevenCompressed: Deadline.SevenCompressed = NoValue.DeadlineSevenCompressed
          override def deadlineFullyCompressed: Deadline.FullyCompressed = NoValue.DeadlineFullyCompressed
          override def deadlineUncompressed: Deadline.Uncompressed = NoValue.DeadlineUncompressed
        }
        object NoValue extends NoValue {
          object NoDeadline extends BaseEntryId(1080) with Deadline.NoDeadline with NoValue
          object DeadlineOneCompressed extends BaseEntryId(1081) with Deadline.OneCompressed with NoValue
          object DeadlineTwoCompressed extends BaseEntryId(1082) with Deadline.TwoCompressed with NoValue
          object DeadlineThreeCompressed extends BaseEntryId(1083) with Deadline.ThreeCompressed with NoValue
          object DeadlineFourCompressed extends BaseEntryId(1084) with Deadline.FourCompressed with NoValue
          object DeadlineFiveCompressed extends BaseEntryId(1085) with Deadline.FiveCompressed with NoValue
          object DeadlineSixCompressed extends BaseEntryId(1086) with Deadline.SixCompressed with NoValue
          object DeadlineSevenCompressed extends BaseEntryId(1087) with Deadline.SevenCompressed with NoValue
          object DeadlineFullyCompressed extends BaseEntryId(1088) with Deadline.FullyCompressed with NoValue
          object DeadlineUncompressed extends BaseEntryId(1089) with Deadline.Uncompressed with NoValue
        }

        trait ValueFullyCompressed extends Value.FullyCompressed with TimePartiallyCompressed {
          override def noDeadline: Deadline.NoDeadline = ValueFullyCompressed.NoDeadline
          override def deadlineOneCompressed: Deadline.OneCompressed = ValueFullyCompressed.DeadlineOneCompressed
          override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueFullyCompressed.DeadlineTwoCompressed
          override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueFullyCompressed.DeadlineThreeCompressed
          override def deadlineFourCompressed: Deadline.FourCompressed = ValueFullyCompressed.DeadlineFourCompressed
          override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueFullyCompressed.DeadlineFiveCompressed
          override def deadlineSixCompressed: Deadline.SixCompressed = ValueFullyCompressed.DeadlineSixCompressed
          override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueFullyCompressed.DeadlineSevenCompressed
          override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueFullyCompressed.DeadlineFullyCompressed
          override def deadlineUncompressed: Deadline.Uncompressed = ValueFullyCompressed.DeadlineUncompressed
        }
        object ValueFullyCompressed extends ValueFullyCompressed {
          object NoDeadline extends BaseEntryId(1090) with Deadline.NoDeadline with ValueFullyCompressed
          object DeadlineOneCompressed extends BaseEntryId(1091) with Deadline.OneCompressed with ValueFullyCompressed
          object DeadlineTwoCompressed extends BaseEntryId(1092) with Deadline.TwoCompressed with ValueFullyCompressed
          object DeadlineThreeCompressed extends BaseEntryId(1093) with Deadline.ThreeCompressed with ValueFullyCompressed
          object DeadlineFourCompressed extends BaseEntryId(1094) with Deadline.FourCompressed with ValueFullyCompressed
          object DeadlineFiveCompressed extends BaseEntryId(1095) with Deadline.FiveCompressed with ValueFullyCompressed
          object DeadlineSixCompressed extends BaseEntryId(1096) with Deadline.SixCompressed with ValueFullyCompressed
          object DeadlineSevenCompressed extends BaseEntryId(1097) with Deadline.SevenCompressed with ValueFullyCompressed
          object DeadlineFullyCompressed extends BaseEntryId(1098) with Deadline.FullyCompressed with ValueFullyCompressed
          object DeadlineUncompressed extends BaseEntryId(1099) with Deadline.Uncompressed with ValueFullyCompressed
        }
      }

      trait TimeFullyCompressed extends Time.FullyCompressed with KeyFullyCompressed {
        override def noValue: Value.NoValue = TimeFullyCompressed.NoValue
        override def valueFullyCompressed: Value.FullyCompressed = TimeFullyCompressed.ValueFullyCompressed
        override def valueUncompressed: Value.Uncompressed = TimeFullyCompressed.ValueUncompressed
      }
      object TimeFullyCompressed extends TimeFullyCompressed {

        trait ValueUncompressed extends Value.Uncompressed with TimeFullyCompressed {
          override def valueOffsetOneCompressed: ValueOffset.OneCompressed = ValueUncompressed.ValueOffsetOneCompressed
          override def valueOffsetTwoCompressed: ValueOffset.TwoCompressed = ValueUncompressed.ValueOffsetTwoCompressed
          override def valueOffsetThreeCompressed: ValueOffset.ThreeCompressed = ValueUncompressed.ValueOffsetThreeCompressed
          override def valueOffsetUncompressed: ValueOffset.Uncompressed = ValueUncompressed.ValueOffsetUncompressed
        }
        object ValueUncompressed extends ValueUncompressed {

          trait ValueOffsetOneCompressed extends ValueOffset.OneCompressed with ValueUncompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetOneCompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetOneCompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetOneCompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetOneCompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetOneCompressed.ValueLengthUncompressed
          }
          object ValueOffsetOneCompressed extends ValueOffsetOneCompressed {
            trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetOneCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthOneCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthOneCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthOneCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthOneCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthOneCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthOneCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthOneCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthOneCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthOneCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthOneCompressed.DeadlineUncompressed
            }

            object ValueLengthOneCompressed extends ValueLengthOneCompressed {
              object NoDeadline extends BaseEntryId(1100) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryId(1101) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1102) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1103) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryId(1104) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1105) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryId(1106) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1107) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1108) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryId(1109) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetOneCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthTwoCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthTwoCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthTwoCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthTwoCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthTwoCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthTwoCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthTwoCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthTwoCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthTwoCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthTwoCompressed.DeadlineUncompressed
            }

            object ValueLengthTwoCompressed extends ValueLengthTwoCompressed {
              object NoDeadline extends BaseEntryId(1110) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryId(1111) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1112) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1113) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryId(1114) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1115) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryId(1116) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1117) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1118) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryId(1119) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetOneCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthThreeCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthThreeCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthThreeCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthThreeCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthThreeCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthThreeCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthThreeCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthThreeCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthThreeCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthThreeCompressed.DeadlineUncompressed
            }

            object ValueLengthThreeCompressed extends ValueLengthThreeCompressed {
              object NoDeadline extends BaseEntryId(1120) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryId(1121) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1122) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1123) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryId(1124) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1125) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryId(1126) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1127) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1128) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryId(1129) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetOneCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthFullyCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthFullyCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthFullyCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthFullyCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthFullyCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthFullyCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthFullyCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthFullyCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthFullyCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthFullyCompressed.DeadlineUncompressed
            }

            object ValueLengthFullyCompressed extends ValueLengthFullyCompressed {
              object NoDeadline extends BaseEntryId(1130) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryId(1131) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1132) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1133) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryId(1134) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1135) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryId(1136) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1137) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1138) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryId(1139) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetOneCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthUncompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthUncompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthUncompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthUncompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthUncompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthUncompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthUncompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthUncompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthUncompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthUncompressed.DeadlineUncompressed
            }

            object ValueLengthUncompressed extends ValueLengthUncompressed {
              object NoDeadline extends BaseEntryId(1140) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryId(1141) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryId(1142) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryId(1143) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryId(1144) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryId(1145) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryId(1146) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryId(1147) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryId(1148) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryId(1149) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }

          trait ValueOffsetTwoCompressed extends ValueOffset.TwoCompressed with ValueUncompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetTwoCompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetTwoCompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetTwoCompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetTwoCompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetTwoCompressed.ValueLengthUncompressed
          }
          object ValueOffsetTwoCompressed extends ValueOffsetTwoCompressed {
            trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetTwoCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthOneCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthOneCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthOneCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthOneCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthOneCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthOneCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthOneCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthOneCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthOneCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthOneCompressed.DeadlineUncompressed
            }

            object ValueLengthOneCompressed extends ValueLengthOneCompressed {
              object NoDeadline extends BaseEntryId(1150) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryId(1151) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1152) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1153) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryId(1154) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1155) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryId(1156) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1157) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1158) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryId(1159) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetTwoCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthTwoCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthTwoCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthTwoCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthTwoCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthTwoCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthTwoCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthTwoCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthTwoCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthTwoCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthTwoCompressed.DeadlineUncompressed
            }

            object ValueLengthTwoCompressed extends ValueLengthTwoCompressed {
              object NoDeadline extends BaseEntryId(1160) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryId(1161) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1162) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1163) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryId(1164) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1165) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryId(1166) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1167) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1168) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryId(1169) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetTwoCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthThreeCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthThreeCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthThreeCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthThreeCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthThreeCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthThreeCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthThreeCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthThreeCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthThreeCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthThreeCompressed.DeadlineUncompressed
            }

            object ValueLengthThreeCompressed extends ValueLengthThreeCompressed {
              object NoDeadline extends BaseEntryId(1170) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryId(1171) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1172) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1173) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryId(1174) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1175) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryId(1176) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1177) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1178) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryId(1179) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetTwoCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthFullyCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthFullyCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthFullyCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthFullyCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthFullyCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthFullyCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthFullyCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthFullyCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthFullyCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthFullyCompressed.DeadlineUncompressed
            }

            object ValueLengthFullyCompressed extends ValueLengthFullyCompressed {
              object NoDeadline extends BaseEntryId(1180) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryId(1181) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1182) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1183) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryId(1184) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1185) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryId(1186) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1187) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1188) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryId(1189) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetTwoCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthUncompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthUncompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthUncompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthUncompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthUncompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthUncompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthUncompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthUncompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthUncompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthUncompressed.DeadlineUncompressed
            }

            object ValueLengthUncompressed extends ValueLengthUncompressed {
              object NoDeadline extends BaseEntryId(1190) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryId(1191) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryId(1192) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryId(1193) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryId(1194) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryId(1195) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryId(1196) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryId(1197) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryId(1198) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryId(1199) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }

          trait ValueOffsetThreeCompressed extends ValueOffset.ThreeCompressed with ValueUncompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetThreeCompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetThreeCompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetThreeCompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetThreeCompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetThreeCompressed.ValueLengthUncompressed
          }
          object ValueOffsetThreeCompressed extends ValueOffsetThreeCompressed {
            trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetThreeCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthOneCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthOneCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthOneCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthOneCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthOneCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthOneCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthOneCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthOneCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthOneCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthOneCompressed.DeadlineUncompressed
            }

            object ValueLengthOneCompressed extends ValueLengthOneCompressed {
              object NoDeadline extends BaseEntryId(1200) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryId(1201) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1202) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1203) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryId(1204) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1205) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryId(1206) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1207) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1208) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryId(1209) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetThreeCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthTwoCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthTwoCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthTwoCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthTwoCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthTwoCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthTwoCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthTwoCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthTwoCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthTwoCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthTwoCompressed.DeadlineUncompressed
            }

            object ValueLengthTwoCompressed extends ValueLengthTwoCompressed {
              object NoDeadline extends BaseEntryId(1210) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryId(1211) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1212) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1213) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryId(1214) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1215) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryId(1216) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1217) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1218) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryId(1219) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetThreeCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthThreeCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthThreeCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthThreeCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthThreeCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthThreeCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthThreeCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthThreeCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthThreeCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthThreeCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthThreeCompressed.DeadlineUncompressed
            }

            object ValueLengthThreeCompressed extends ValueLengthThreeCompressed {
              object NoDeadline extends BaseEntryId(1220) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryId(1221) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1222) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1223) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryId(1224) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1225) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryId(1226) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1227) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1228) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryId(1229) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetThreeCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthFullyCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthFullyCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthFullyCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthFullyCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthFullyCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthFullyCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthFullyCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthFullyCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthFullyCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthFullyCompressed.DeadlineUncompressed
            }

            object ValueLengthFullyCompressed extends ValueLengthFullyCompressed {
              object NoDeadline extends BaseEntryId(1230) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryId(1231) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1232) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1233) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryId(1234) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1235) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryId(1236) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1237) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1238) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryId(1239) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetThreeCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthUncompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthUncompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthUncompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthUncompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthUncompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthUncompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthUncompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthUncompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthUncompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthUncompressed.DeadlineUncompressed
            }

            object ValueLengthUncompressed extends ValueLengthUncompressed {
              object NoDeadline extends BaseEntryId(1240) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryId(1241) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryId(1242) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryId(1243) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryId(1244) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryId(1245) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryId(1246) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryId(1247) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryId(1248) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryId(1249) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }

          trait ValueOffsetUncompressed extends ValueOffset.Uncompressed with ValueUncompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetUncompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetUncompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetUncompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetUncompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetUncompressed.ValueLengthUncompressed
          }
          object ValueOffsetUncompressed extends ValueOffsetUncompressed {
            trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetUncompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthOneCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthOneCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthOneCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthOneCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthOneCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthOneCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthOneCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthOneCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthOneCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthOneCompressed.DeadlineUncompressed
            }

            object ValueLengthOneCompressed extends ValueLengthOneCompressed {
              object NoDeadline extends BaseEntryId(1250) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryId(1251) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1252) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1253) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryId(1254) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1255) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryId(1256) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1257) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1258) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryId(1259) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetUncompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthTwoCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthTwoCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthTwoCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthTwoCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthTwoCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthTwoCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthTwoCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthTwoCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthTwoCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthTwoCompressed.DeadlineUncompressed
            }

            object ValueLengthTwoCompressed extends ValueLengthTwoCompressed {
              object NoDeadline extends BaseEntryId(1260) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryId(1261) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1262) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1263) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryId(1264) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1265) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryId(1266) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1267) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1268) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryId(1269) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetUncompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthThreeCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthThreeCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthThreeCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthThreeCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthThreeCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthThreeCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthThreeCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthThreeCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthThreeCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthThreeCompressed.DeadlineUncompressed
            }

            object ValueLengthThreeCompressed extends ValueLengthThreeCompressed {
              object NoDeadline extends BaseEntryId(1270) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryId(1271) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1272) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1273) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryId(1274) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1275) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryId(1276) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1277) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1278) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryId(1279) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetUncompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthFullyCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthFullyCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthFullyCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthFullyCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthFullyCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthFullyCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthFullyCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthFullyCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthFullyCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthFullyCompressed.DeadlineUncompressed
            }

            object ValueLengthFullyCompressed extends ValueLengthFullyCompressed {
              object NoDeadline extends BaseEntryId(1280) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryId(1281) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1282) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1283) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryId(1284) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1285) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryId(1286) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1287) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1288) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryId(1289) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetUncompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthUncompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthUncompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthUncompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthUncompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthUncompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthUncompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthUncompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthUncompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthUncompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthUncompressed.DeadlineUncompressed
            }

            object ValueLengthUncompressed extends ValueLengthUncompressed {
              object NoDeadline extends BaseEntryId(1290) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryId(1291) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryId(1292) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryId(1293) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryId(1294) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryId(1295) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryId(1296) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryId(1297) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryId(1298) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryId(1299) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }
        }

        trait NoValue extends Value.NoValue with TimeFullyCompressed {
          override def noDeadline: Deadline.NoDeadline = NoValue.NoDeadline
          override def deadlineOneCompressed: Deadline.OneCompressed = NoValue.DeadlineOneCompressed
          override def deadlineTwoCompressed: Deadline.TwoCompressed = NoValue.DeadlineTwoCompressed
          override def deadlineThreeCompressed: Deadline.ThreeCompressed = NoValue.DeadlineThreeCompressed
          override def deadlineFourCompressed: Deadline.FourCompressed = NoValue.DeadlineFourCompressed
          override def deadlineFiveCompressed: Deadline.FiveCompressed = NoValue.DeadlineFiveCompressed
          override def deadlineSixCompressed: Deadline.SixCompressed = NoValue.DeadlineSixCompressed
          override def deadlineSevenCompressed: Deadline.SevenCompressed = NoValue.DeadlineSevenCompressed
          override def deadlineFullyCompressed: Deadline.FullyCompressed = NoValue.DeadlineFullyCompressed
          override def deadlineUncompressed: Deadline.Uncompressed = NoValue.DeadlineUncompressed
        }
        object NoValue extends NoValue {
          object NoDeadline extends BaseEntryId(1300) with Deadline.NoDeadline with NoValue
          object DeadlineOneCompressed extends BaseEntryId(1301) with Deadline.OneCompressed with NoValue
          object DeadlineTwoCompressed extends BaseEntryId(1302) with Deadline.TwoCompressed with NoValue
          object DeadlineThreeCompressed extends BaseEntryId(1303) with Deadline.ThreeCompressed with NoValue
          object DeadlineFourCompressed extends BaseEntryId(1304) with Deadline.FourCompressed with NoValue
          object DeadlineFiveCompressed extends BaseEntryId(1305) with Deadline.FiveCompressed with NoValue
          object DeadlineSixCompressed extends BaseEntryId(1306) with Deadline.SixCompressed with NoValue
          object DeadlineSevenCompressed extends BaseEntryId(1307) with Deadline.SevenCompressed with NoValue
          object DeadlineFullyCompressed extends BaseEntryId(1308) with Deadline.FullyCompressed with NoValue
          object DeadlineUncompressed extends BaseEntryId(1309) with Deadline.Uncompressed with NoValue
        }

        trait ValueFullyCompressed extends Value.FullyCompressed with TimeFullyCompressed {
          override def noDeadline: Deadline.NoDeadline = ValueFullyCompressed.NoDeadline
          override def deadlineOneCompressed: Deadline.OneCompressed = ValueFullyCompressed.DeadlineOneCompressed
          override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueFullyCompressed.DeadlineTwoCompressed
          override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueFullyCompressed.DeadlineThreeCompressed
          override def deadlineFourCompressed: Deadline.FourCompressed = ValueFullyCompressed.DeadlineFourCompressed
          override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueFullyCompressed.DeadlineFiveCompressed
          override def deadlineSixCompressed: Deadline.SixCompressed = ValueFullyCompressed.DeadlineSixCompressed
          override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueFullyCompressed.DeadlineSevenCompressed
          override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueFullyCompressed.DeadlineFullyCompressed
          override def deadlineUncompressed: Deadline.Uncompressed = ValueFullyCompressed.DeadlineUncompressed
        }
        object ValueFullyCompressed extends ValueFullyCompressed {
          object NoDeadline extends BaseEntryId(1310) with Deadline.NoDeadline with ValueFullyCompressed
          object DeadlineOneCompressed extends BaseEntryId(1311) with Deadline.OneCompressed with ValueFullyCompressed
          object DeadlineTwoCompressed extends BaseEntryId(1312) with Deadline.TwoCompressed with ValueFullyCompressed
          object DeadlineThreeCompressed extends BaseEntryId(1313) with Deadline.ThreeCompressed with ValueFullyCompressed
          object DeadlineFourCompressed extends BaseEntryId(1314) with Deadline.FourCompressed with ValueFullyCompressed
          object DeadlineFiveCompressed extends BaseEntryId(1315) with Deadline.FiveCompressed with ValueFullyCompressed
          object DeadlineSixCompressed extends BaseEntryId(1316) with Deadline.SixCompressed with ValueFullyCompressed
          object DeadlineSevenCompressed extends BaseEntryId(1317) with Deadline.SevenCompressed with ValueFullyCompressed
          object DeadlineFullyCompressed extends BaseEntryId(1318) with Deadline.FullyCompressed with ValueFullyCompressed
          object DeadlineUncompressed extends BaseEntryId(1319) with Deadline.Uncompressed with ValueFullyCompressed
        }
      }

      trait TimeUncompressed extends Time.Uncompressed with KeyFullyCompressed {
        override def noValue: Value.NoValue = TimeUncompressed.NoValue
        override def valueFullyCompressed: Value.FullyCompressed = TimeUncompressed.ValueFullyCompressed
        override def valueUncompressed: Value.Uncompressed = TimeUncompressed.ValueUncompressed
      }
      object TimeUncompressed extends TimeUncompressed {

        trait ValueUncompressed extends Value.Uncompressed with TimeUncompressed {
          override def valueOffsetOneCompressed: ValueOffset.OneCompressed = ValueUncompressed.ValueOffsetOneCompressed
          override def valueOffsetTwoCompressed: ValueOffset.TwoCompressed = ValueUncompressed.ValueOffsetTwoCompressed
          override def valueOffsetThreeCompressed: ValueOffset.ThreeCompressed = ValueUncompressed.ValueOffsetThreeCompressed
          override def valueOffsetUncompressed: ValueOffset.Uncompressed = ValueUncompressed.ValueOffsetUncompressed
        }
        object ValueUncompressed extends ValueUncompressed {

          trait ValueOffsetOneCompressed extends ValueOffset.OneCompressed with ValueUncompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetOneCompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetOneCompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetOneCompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetOneCompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetOneCompressed.ValueLengthUncompressed
          }
          object ValueOffsetOneCompressed extends ValueOffsetOneCompressed {
            trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetOneCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthOneCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthOneCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthOneCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthOneCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthOneCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthOneCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthOneCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthOneCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthOneCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthOneCompressed.DeadlineUncompressed
            }

            object ValueLengthOneCompressed extends ValueLengthOneCompressed {
              object NoDeadline extends BaseEntryId(1320) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryId(1321) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1322) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1323) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryId(1324) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1325) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryId(1326) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1327) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1328) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryId(1329) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetOneCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthTwoCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthTwoCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthTwoCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthTwoCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthTwoCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthTwoCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthTwoCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthTwoCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthTwoCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthTwoCompressed.DeadlineUncompressed
            }

            object ValueLengthTwoCompressed extends ValueLengthTwoCompressed {
              object NoDeadline extends BaseEntryId(1330) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryId(1331) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1332) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1333) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryId(1334) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1335) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryId(1336) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1337) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1338) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryId(1339) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetOneCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthThreeCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthThreeCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthThreeCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthThreeCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthThreeCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthThreeCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthThreeCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthThreeCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthThreeCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthThreeCompressed.DeadlineUncompressed
            }

            object ValueLengthThreeCompressed extends ValueLengthThreeCompressed {
              object NoDeadline extends BaseEntryId(1340) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryId(1341) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1342) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1343) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryId(1344) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1345) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryId(1346) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1347) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1348) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryId(1349) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetOneCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthFullyCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthFullyCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthFullyCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthFullyCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthFullyCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthFullyCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthFullyCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthFullyCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthFullyCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthFullyCompressed.DeadlineUncompressed
            }

            object ValueLengthFullyCompressed extends ValueLengthFullyCompressed {
              object NoDeadline extends BaseEntryId(1350) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryId(1351) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1352) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1353) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryId(1354) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1355) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryId(1356) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1357) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1358) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryId(1359) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetOneCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthUncompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthUncompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthUncompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthUncompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthUncompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthUncompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthUncompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthUncompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthUncompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthUncompressed.DeadlineUncompressed
            }

            object ValueLengthUncompressed extends ValueLengthUncompressed {
              object NoDeadline extends BaseEntryId(1360) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryId(1361) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryId(1362) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryId(1363) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryId(1364) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryId(1365) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryId(1366) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryId(1367) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryId(1368) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryId(1369) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }

          trait ValueOffsetTwoCompressed extends ValueOffset.TwoCompressed with ValueUncompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetTwoCompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetTwoCompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetTwoCompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetTwoCompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetTwoCompressed.ValueLengthUncompressed
          }
          object ValueOffsetTwoCompressed extends ValueOffsetTwoCompressed {
            trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetTwoCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthOneCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthOneCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthOneCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthOneCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthOneCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthOneCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthOneCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthOneCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthOneCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthOneCompressed.DeadlineUncompressed
            }

            object ValueLengthOneCompressed extends ValueLengthOneCompressed {
              object NoDeadline extends BaseEntryId(1370) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryId(1371) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1372) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1373) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryId(1374) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1375) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryId(1376) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1377) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1378) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryId(1379) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetTwoCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthTwoCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthTwoCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthTwoCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthTwoCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthTwoCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthTwoCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthTwoCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthTwoCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthTwoCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthTwoCompressed.DeadlineUncompressed
            }

            object ValueLengthTwoCompressed extends ValueLengthTwoCompressed {
              object NoDeadline extends BaseEntryId(1380) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryId(1381) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1382) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1383) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryId(1384) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1385) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryId(1386) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1387) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1388) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryId(1389) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetTwoCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthThreeCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthThreeCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthThreeCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthThreeCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthThreeCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthThreeCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthThreeCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthThreeCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthThreeCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthThreeCompressed.DeadlineUncompressed
            }

            object ValueLengthThreeCompressed extends ValueLengthThreeCompressed {
              object NoDeadline extends BaseEntryId(1390) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryId(1391) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1392) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1393) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryId(1394) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1395) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryId(1396) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1397) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1398) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryId(1399) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetTwoCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthFullyCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthFullyCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthFullyCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthFullyCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthFullyCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthFullyCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthFullyCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthFullyCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthFullyCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthFullyCompressed.DeadlineUncompressed
            }

            object ValueLengthFullyCompressed extends ValueLengthFullyCompressed {
              object NoDeadline extends BaseEntryId(1400) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryId(1401) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1402) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1403) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryId(1404) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1405) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryId(1406) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1407) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1408) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryId(1409) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetTwoCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthUncompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthUncompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthUncompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthUncompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthUncompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthUncompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthUncompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthUncompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthUncompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthUncompressed.DeadlineUncompressed
            }

            object ValueLengthUncompressed extends ValueLengthUncompressed {
              object NoDeadline extends BaseEntryId(1410) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryId(1411) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryId(1412) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryId(1413) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryId(1414) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryId(1415) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryId(1416) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryId(1417) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryId(1418) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryId(1419) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }

          trait ValueOffsetThreeCompressed extends ValueOffset.ThreeCompressed with ValueUncompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetThreeCompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetThreeCompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetThreeCompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetThreeCompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetThreeCompressed.ValueLengthUncompressed
          }
          object ValueOffsetThreeCompressed extends ValueOffsetThreeCompressed {
            trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetThreeCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthOneCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthOneCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthOneCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthOneCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthOneCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthOneCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthOneCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthOneCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthOneCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthOneCompressed.DeadlineUncompressed
            }

            object ValueLengthOneCompressed extends ValueLengthOneCompressed {
              object NoDeadline extends BaseEntryId(1420) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryId(1421) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1422) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1423) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryId(1424) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1425) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryId(1426) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1427) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1428) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryId(1429) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetThreeCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthTwoCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthTwoCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthTwoCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthTwoCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthTwoCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthTwoCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthTwoCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthTwoCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthTwoCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthTwoCompressed.DeadlineUncompressed
            }

            object ValueLengthTwoCompressed extends ValueLengthTwoCompressed {
              object NoDeadline extends BaseEntryId(1430) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryId(1431) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1432) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1433) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryId(1434) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1435) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryId(1436) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1437) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1438) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryId(1439) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetThreeCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthThreeCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthThreeCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthThreeCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthThreeCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthThreeCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthThreeCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthThreeCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthThreeCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthThreeCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthThreeCompressed.DeadlineUncompressed
            }

            object ValueLengthThreeCompressed extends ValueLengthThreeCompressed {
              object NoDeadline extends BaseEntryId(1440) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryId(1441) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1442) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1443) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryId(1444) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1445) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryId(1446) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1447) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1448) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryId(1449) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetThreeCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthFullyCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthFullyCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthFullyCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthFullyCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthFullyCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthFullyCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthFullyCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthFullyCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthFullyCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthFullyCompressed.DeadlineUncompressed
            }

            object ValueLengthFullyCompressed extends ValueLengthFullyCompressed {
              object NoDeadline extends BaseEntryId(1450) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryId(1451) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1452) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1453) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryId(1454) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1455) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryId(1456) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1457) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1458) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryId(1459) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetThreeCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthUncompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthUncompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthUncompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthUncompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthUncompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthUncompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthUncompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthUncompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthUncompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthUncompressed.DeadlineUncompressed
            }

            object ValueLengthUncompressed extends ValueLengthUncompressed {
              object NoDeadline extends BaseEntryId(1460) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryId(1461) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryId(1462) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryId(1463) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryId(1464) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryId(1465) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryId(1466) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryId(1467) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryId(1468) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryId(1469) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }

          trait ValueOffsetUncompressed extends ValueOffset.Uncompressed with ValueUncompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetUncompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetUncompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetUncompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetUncompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetUncompressed.ValueLengthUncompressed
          }
          object ValueOffsetUncompressed extends ValueOffsetUncompressed {
            trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetUncompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthOneCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthOneCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthOneCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthOneCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthOneCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthOneCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthOneCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthOneCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthOneCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthOneCompressed.DeadlineUncompressed
            }

            object ValueLengthOneCompressed extends ValueLengthOneCompressed {
              object NoDeadline extends BaseEntryId(1470) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryId(1471) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1472) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1473) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryId(1474) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1475) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryId(1476) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1477) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1478) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryId(1479) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetUncompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthTwoCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthTwoCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthTwoCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthTwoCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthTwoCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthTwoCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthTwoCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthTwoCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthTwoCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthTwoCompressed.DeadlineUncompressed
            }

            object ValueLengthTwoCompressed extends ValueLengthTwoCompressed {
              object NoDeadline extends BaseEntryId(1480) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryId(1481) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1482) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1483) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryId(1484) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1485) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryId(1486) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1487) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1488) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryId(1489) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetUncompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthThreeCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthThreeCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthThreeCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthThreeCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthThreeCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthThreeCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthThreeCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthThreeCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthThreeCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthThreeCompressed.DeadlineUncompressed
            }

            object ValueLengthThreeCompressed extends ValueLengthThreeCompressed {
              object NoDeadline extends BaseEntryId(1490) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryId(1491) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1492) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1493) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryId(1494) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1495) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryId(1496) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1497) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1498) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryId(1499) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetUncompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthFullyCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthFullyCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthFullyCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthFullyCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthFullyCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthFullyCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthFullyCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthFullyCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthFullyCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthFullyCompressed.DeadlineUncompressed
            }

            object ValueLengthFullyCompressed extends ValueLengthFullyCompressed {
              object NoDeadline extends BaseEntryId(1500) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryId(1501) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1502) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1503) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryId(1504) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1505) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryId(1506) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1507) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1508) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryId(1509) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetUncompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthUncompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthUncompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthUncompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthUncompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthUncompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthUncompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthUncompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthUncompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthUncompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthUncompressed.DeadlineUncompressed
            }

            object ValueLengthUncompressed extends ValueLengthUncompressed {
              object NoDeadline extends BaseEntryId(1510) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryId(1511) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryId(1512) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryId(1513) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryId(1514) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryId(1515) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryId(1516) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryId(1517) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryId(1518) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryId(1519) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }
        }

        trait NoValue extends Value.NoValue with TimeUncompressed {
          override def noDeadline: Deadline.NoDeadline = NoValue.NoDeadline
          override def deadlineOneCompressed: Deadline.OneCompressed = NoValue.DeadlineOneCompressed
          override def deadlineTwoCompressed: Deadline.TwoCompressed = NoValue.DeadlineTwoCompressed
          override def deadlineThreeCompressed: Deadline.ThreeCompressed = NoValue.DeadlineThreeCompressed
          override def deadlineFourCompressed: Deadline.FourCompressed = NoValue.DeadlineFourCompressed
          override def deadlineFiveCompressed: Deadline.FiveCompressed = NoValue.DeadlineFiveCompressed
          override def deadlineSixCompressed: Deadline.SixCompressed = NoValue.DeadlineSixCompressed
          override def deadlineSevenCompressed: Deadline.SevenCompressed = NoValue.DeadlineSevenCompressed
          override def deadlineFullyCompressed: Deadline.FullyCompressed = NoValue.DeadlineFullyCompressed
          override def deadlineUncompressed: Deadline.Uncompressed = NoValue.DeadlineUncompressed
        }
        object NoValue extends NoValue {
          object NoDeadline extends BaseEntryId(1520) with Deadline.NoDeadline with NoValue
          object DeadlineOneCompressed extends BaseEntryId(1521) with Deadline.OneCompressed with NoValue
          object DeadlineTwoCompressed extends BaseEntryId(1522) with Deadline.TwoCompressed with NoValue
          object DeadlineThreeCompressed extends BaseEntryId(1523) with Deadline.ThreeCompressed with NoValue
          object DeadlineFourCompressed extends BaseEntryId(1524) with Deadline.FourCompressed with NoValue
          object DeadlineFiveCompressed extends BaseEntryId(1525) with Deadline.FiveCompressed with NoValue
          object DeadlineSixCompressed extends BaseEntryId(1526) with Deadline.SixCompressed with NoValue
          object DeadlineSevenCompressed extends BaseEntryId(1527) with Deadline.SevenCompressed with NoValue
          object DeadlineFullyCompressed extends BaseEntryId(1528) with Deadline.FullyCompressed with NoValue
          object DeadlineUncompressed extends BaseEntryId(1529) with Deadline.Uncompressed with NoValue
        }

        trait ValueFullyCompressed extends Value.FullyCompressed with TimeUncompressed {
          override def noDeadline: Deadline.NoDeadline = ValueFullyCompressed.NoDeadline
          override def deadlineOneCompressed: Deadline.OneCompressed = ValueFullyCompressed.DeadlineOneCompressed
          override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueFullyCompressed.DeadlineTwoCompressed
          override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueFullyCompressed.DeadlineThreeCompressed
          override def deadlineFourCompressed: Deadline.FourCompressed = ValueFullyCompressed.DeadlineFourCompressed
          override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueFullyCompressed.DeadlineFiveCompressed
          override def deadlineSixCompressed: Deadline.SixCompressed = ValueFullyCompressed.DeadlineSixCompressed
          override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueFullyCompressed.DeadlineSevenCompressed
          override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueFullyCompressed.DeadlineFullyCompressed
          override def deadlineUncompressed: Deadline.Uncompressed = ValueFullyCompressed.DeadlineUncompressed
        }
        object ValueFullyCompressed extends ValueFullyCompressed {
          object NoDeadline extends BaseEntryId(1530) with Deadline.NoDeadline with ValueFullyCompressed
          object DeadlineOneCompressed extends BaseEntryId(1531) with Deadline.OneCompressed with ValueFullyCompressed
          object DeadlineTwoCompressed extends BaseEntryId(1532) with Deadline.TwoCompressed with ValueFullyCompressed
          object DeadlineThreeCompressed extends BaseEntryId(1533) with Deadline.ThreeCompressed with ValueFullyCompressed
          object DeadlineFourCompressed extends BaseEntryId(1534) with Deadline.FourCompressed with ValueFullyCompressed
          object DeadlineFiveCompressed extends BaseEntryId(1535) with Deadline.FiveCompressed with ValueFullyCompressed
          object DeadlineSixCompressed extends BaseEntryId(1536) with Deadline.SixCompressed with ValueFullyCompressed
          object DeadlineSevenCompressed extends BaseEntryId(1537) with Deadline.SevenCompressed with ValueFullyCompressed
          object DeadlineFullyCompressed extends BaseEntryId(1538) with Deadline.FullyCompressed with ValueFullyCompressed
          object DeadlineUncompressed extends BaseEntryId(1539) with Deadline.Uncompressed with ValueFullyCompressed
        }
      }

      trait NoTime extends Time.NoTime with KeyFullyCompressed {
        override def noValue: Value.NoValue = NoTime.NoValue
        override def valueFullyCompressed: Value.FullyCompressed = NoTime.ValueFullyCompressed
        override def valueUncompressed: Value.Uncompressed = NoTime.ValueUncompressed
      }
      object NoTime extends NoTime {

        trait ValueUncompressed extends Value.Uncompressed with NoTime {
          override def valueOffsetOneCompressed: ValueOffset.OneCompressed = ValueUncompressed.ValueOffsetOneCompressed
          override def valueOffsetTwoCompressed: ValueOffset.TwoCompressed = ValueUncompressed.ValueOffsetTwoCompressed
          override def valueOffsetThreeCompressed: ValueOffset.ThreeCompressed = ValueUncompressed.ValueOffsetThreeCompressed
          override def valueOffsetUncompressed: ValueOffset.Uncompressed = ValueUncompressed.ValueOffsetUncompressed
        }
        object ValueUncompressed extends ValueUncompressed {

          trait ValueOffsetOneCompressed extends ValueOffset.OneCompressed with ValueUncompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetOneCompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetOneCompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetOneCompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetOneCompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetOneCompressed.ValueLengthUncompressed
          }
          object ValueOffsetOneCompressed extends ValueOffsetOneCompressed {
            trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetOneCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthOneCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthOneCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthOneCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthOneCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthOneCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthOneCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthOneCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthOneCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthOneCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthOneCompressed.DeadlineUncompressed
            }

            object ValueLengthOneCompressed extends ValueLengthOneCompressed {
              object NoDeadline extends BaseEntryId(1540) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryId(1541) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1542) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1543) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryId(1544) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1545) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryId(1546) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1547) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1548) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryId(1549) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetOneCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthTwoCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthTwoCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthTwoCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthTwoCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthTwoCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthTwoCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthTwoCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthTwoCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthTwoCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthTwoCompressed.DeadlineUncompressed
            }

            object ValueLengthTwoCompressed extends ValueLengthTwoCompressed {
              object NoDeadline extends BaseEntryId(1550) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryId(1551) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1552) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1553) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryId(1554) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1555) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryId(1556) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1557) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1558) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryId(1559) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetOneCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthThreeCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthThreeCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthThreeCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthThreeCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthThreeCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthThreeCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthThreeCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthThreeCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthThreeCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthThreeCompressed.DeadlineUncompressed
            }

            object ValueLengthThreeCompressed extends ValueLengthThreeCompressed {
              object NoDeadline extends BaseEntryId(1560) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryId(1561) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1562) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1563) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryId(1564) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1565) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryId(1566) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1567) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1568) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryId(1569) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetOneCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthFullyCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthFullyCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthFullyCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthFullyCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthFullyCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthFullyCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthFullyCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthFullyCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthFullyCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthFullyCompressed.DeadlineUncompressed
            }

            object ValueLengthFullyCompressed extends ValueLengthFullyCompressed {
              object NoDeadline extends BaseEntryId(1570) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryId(1571) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1572) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1573) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryId(1574) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1575) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryId(1576) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1577) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1578) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryId(1579) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetOneCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthUncompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthUncompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthUncompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthUncompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthUncompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthUncompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthUncompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthUncompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthUncompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthUncompressed.DeadlineUncompressed
            }

            object ValueLengthUncompressed extends ValueLengthUncompressed {
              object NoDeadline extends BaseEntryId(1580) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryId(1581) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryId(1582) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryId(1583) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryId(1584) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryId(1585) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryId(1586) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryId(1587) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryId(1588) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryId(1589) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }

          trait ValueOffsetTwoCompressed extends ValueOffset.TwoCompressed with ValueUncompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetTwoCompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetTwoCompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetTwoCompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetTwoCompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetTwoCompressed.ValueLengthUncompressed
          }
          object ValueOffsetTwoCompressed extends ValueOffsetTwoCompressed {
            trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetTwoCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthOneCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthOneCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthOneCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthOneCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthOneCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthOneCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthOneCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthOneCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthOneCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthOneCompressed.DeadlineUncompressed
            }

            object ValueLengthOneCompressed extends ValueLengthOneCompressed {
              object NoDeadline extends BaseEntryId(1590) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryId(1591) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1592) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1593) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryId(1594) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1595) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryId(1596) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1597) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1598) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryId(1599) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetTwoCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthTwoCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthTwoCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthTwoCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthTwoCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthTwoCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthTwoCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthTwoCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthTwoCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthTwoCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthTwoCompressed.DeadlineUncompressed
            }

            object ValueLengthTwoCompressed extends ValueLengthTwoCompressed {
              object NoDeadline extends BaseEntryId(1600) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryId(1601) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1602) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1603) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryId(1604) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1605) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryId(1606) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1607) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1608) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryId(1609) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetTwoCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthThreeCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthThreeCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthThreeCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthThreeCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthThreeCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthThreeCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthThreeCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthThreeCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthThreeCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthThreeCompressed.DeadlineUncompressed
            }

            object ValueLengthThreeCompressed extends ValueLengthThreeCompressed {
              object NoDeadline extends BaseEntryId(1610) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryId(1611) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1612) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1613) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryId(1614) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1615) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryId(1616) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1617) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1618) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryId(1619) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetTwoCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthFullyCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthFullyCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthFullyCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthFullyCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthFullyCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthFullyCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthFullyCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthFullyCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthFullyCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthFullyCompressed.DeadlineUncompressed
            }

            object ValueLengthFullyCompressed extends ValueLengthFullyCompressed {
              object NoDeadline extends BaseEntryId(1620) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryId(1621) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1622) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1623) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryId(1624) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1625) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryId(1626) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1627) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1628) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryId(1629) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetTwoCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthUncompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthUncompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthUncompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthUncompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthUncompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthUncompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthUncompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthUncompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthUncompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthUncompressed.DeadlineUncompressed
            }

            object ValueLengthUncompressed extends ValueLengthUncompressed {
              object NoDeadline extends BaseEntryId(1630) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryId(1631) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryId(1632) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryId(1633) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryId(1634) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryId(1635) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryId(1636) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryId(1637) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryId(1638) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryId(1639) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }

          trait ValueOffsetThreeCompressed extends ValueOffset.ThreeCompressed with ValueUncompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetThreeCompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetThreeCompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetThreeCompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetThreeCompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetThreeCompressed.ValueLengthUncompressed
          }
          object ValueOffsetThreeCompressed extends ValueOffsetThreeCompressed {
            trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetThreeCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthOneCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthOneCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthOneCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthOneCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthOneCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthOneCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthOneCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthOneCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthOneCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthOneCompressed.DeadlineUncompressed
            }

            object ValueLengthOneCompressed extends ValueLengthOneCompressed {
              object NoDeadline extends BaseEntryId(1640) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryId(1641) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1642) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1643) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryId(1644) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1645) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryId(1646) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1647) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1648) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryId(1649) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetThreeCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthTwoCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthTwoCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthTwoCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthTwoCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthTwoCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthTwoCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthTwoCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthTwoCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthTwoCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthTwoCompressed.DeadlineUncompressed
            }

            object ValueLengthTwoCompressed extends ValueLengthTwoCompressed {
              object NoDeadline extends BaseEntryId(1650) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryId(1651) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1652) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1653) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryId(1654) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1655) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryId(1656) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1657) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1658) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryId(1659) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetThreeCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthThreeCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthThreeCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthThreeCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthThreeCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthThreeCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthThreeCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthThreeCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthThreeCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthThreeCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthThreeCompressed.DeadlineUncompressed
            }

            object ValueLengthThreeCompressed extends ValueLengthThreeCompressed {
              object NoDeadline extends BaseEntryId(1660) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryId(1661) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1662) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1663) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryId(1664) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1665) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryId(1666) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1667) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1668) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryId(1669) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetThreeCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthFullyCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthFullyCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthFullyCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthFullyCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthFullyCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthFullyCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthFullyCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthFullyCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthFullyCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthFullyCompressed.DeadlineUncompressed
            }

            object ValueLengthFullyCompressed extends ValueLengthFullyCompressed {
              object NoDeadline extends BaseEntryId(1670) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryId(1671) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1672) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1673) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryId(1674) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1675) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryId(1676) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1677) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1678) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryId(1679) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetThreeCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthUncompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthUncompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthUncompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthUncompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthUncompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthUncompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthUncompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthUncompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthUncompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthUncompressed.DeadlineUncompressed
            }

            object ValueLengthUncompressed extends ValueLengthUncompressed {
              object NoDeadline extends BaseEntryId(1680) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryId(1681) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryId(1682) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryId(1683) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryId(1684) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryId(1685) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryId(1686) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryId(1687) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryId(1688) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryId(1689) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }

          trait ValueOffsetUncompressed extends ValueOffset.Uncompressed with ValueUncompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetUncompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetUncompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetUncompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetUncompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetUncompressed.ValueLengthUncompressed
          }
          object ValueOffsetUncompressed extends ValueOffsetUncompressed {
            trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetUncompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthOneCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthOneCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthOneCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthOneCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthOneCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthOneCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthOneCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthOneCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthOneCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthOneCompressed.DeadlineUncompressed
            }

            object ValueLengthOneCompressed extends ValueLengthOneCompressed {
              object NoDeadline extends BaseEntryId(1690) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryId(1691) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1692) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1693) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryId(1694) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1695) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryId(1696) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1697) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1698) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryId(1699) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetUncompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthTwoCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthTwoCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthTwoCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthTwoCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthTwoCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthTwoCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthTwoCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthTwoCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthTwoCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthTwoCompressed.DeadlineUncompressed
            }

            object ValueLengthTwoCompressed extends ValueLengthTwoCompressed {
              object NoDeadline extends BaseEntryId(1700) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryId(1701) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1702) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1703) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryId(1704) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1705) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryId(1706) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1707) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1708) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryId(1709) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetUncompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthThreeCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthThreeCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthThreeCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthThreeCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthThreeCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthThreeCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthThreeCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthThreeCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthThreeCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthThreeCompressed.DeadlineUncompressed
            }

            object ValueLengthThreeCompressed extends ValueLengthThreeCompressed {
              object NoDeadline extends BaseEntryId(1710) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryId(1711) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1712) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1713) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryId(1714) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1715) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryId(1716) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1717) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1718) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryId(1719) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetUncompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthFullyCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthFullyCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthFullyCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthFullyCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthFullyCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthFullyCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthFullyCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthFullyCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthFullyCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthFullyCompressed.DeadlineUncompressed
            }

            object ValueLengthFullyCompressed extends ValueLengthFullyCompressed {
              object NoDeadline extends BaseEntryId(1720) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryId(1721) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1722) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1723) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryId(1724) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1725) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryId(1726) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1727) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1728) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryId(1729) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetUncompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthUncompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthUncompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthUncompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthUncompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthUncompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthUncompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthUncompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthUncompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthUncompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthUncompressed.DeadlineUncompressed
            }

            object ValueLengthUncompressed extends ValueLengthUncompressed {
              object NoDeadline extends BaseEntryId(1730) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryId(1731) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryId(1732) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryId(1733) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryId(1734) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryId(1735) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryId(1736) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryId(1737) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryId(1738) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryId(1739) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }
        }

        trait NoValue extends Value.NoValue with NoTime {
          override def noDeadline: Deadline.NoDeadline = NoValue.NoDeadline
          override def deadlineOneCompressed: Deadline.OneCompressed = NoValue.DeadlineOneCompressed
          override def deadlineTwoCompressed: Deadline.TwoCompressed = NoValue.DeadlineTwoCompressed
          override def deadlineThreeCompressed: Deadline.ThreeCompressed = NoValue.DeadlineThreeCompressed
          override def deadlineFourCompressed: Deadline.FourCompressed = NoValue.DeadlineFourCompressed
          override def deadlineFiveCompressed: Deadline.FiveCompressed = NoValue.DeadlineFiveCompressed
          override def deadlineSixCompressed: Deadline.SixCompressed = NoValue.DeadlineSixCompressed
          override def deadlineSevenCompressed: Deadline.SevenCompressed = NoValue.DeadlineSevenCompressed
          override def deadlineFullyCompressed: Deadline.FullyCompressed = NoValue.DeadlineFullyCompressed
          override def deadlineUncompressed: Deadline.Uncompressed = NoValue.DeadlineUncompressed
        }
        object NoValue extends NoValue {
          object NoDeadline extends BaseEntryId(1740) with Deadline.NoDeadline with NoValue
          object DeadlineOneCompressed extends BaseEntryId(1741) with Deadline.OneCompressed with NoValue
          object DeadlineTwoCompressed extends BaseEntryId(1742) with Deadline.TwoCompressed with NoValue
          object DeadlineThreeCompressed extends BaseEntryId(1743) with Deadline.ThreeCompressed with NoValue
          object DeadlineFourCompressed extends BaseEntryId(1744) with Deadline.FourCompressed with NoValue
          object DeadlineFiveCompressed extends BaseEntryId(1745) with Deadline.FiveCompressed with NoValue
          object DeadlineSixCompressed extends BaseEntryId(1746) with Deadline.SixCompressed with NoValue
          object DeadlineSevenCompressed extends BaseEntryId(1747) with Deadline.SevenCompressed with NoValue
          object DeadlineFullyCompressed extends BaseEntryId(1748) with Deadline.FullyCompressed with NoValue
          object DeadlineUncompressed extends BaseEntryId(1749) with Deadline.Uncompressed with NoValue
        }

        trait ValueFullyCompressed extends Value.FullyCompressed with NoTime {
          override def noDeadline: Deadline.NoDeadline = ValueFullyCompressed.NoDeadline
          override def deadlineOneCompressed: Deadline.OneCompressed = ValueFullyCompressed.DeadlineOneCompressed
          override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueFullyCompressed.DeadlineTwoCompressed
          override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueFullyCompressed.DeadlineThreeCompressed
          override def deadlineFourCompressed: Deadline.FourCompressed = ValueFullyCompressed.DeadlineFourCompressed
          override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueFullyCompressed.DeadlineFiveCompressed
          override def deadlineSixCompressed: Deadline.SixCompressed = ValueFullyCompressed.DeadlineSixCompressed
          override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueFullyCompressed.DeadlineSevenCompressed
          override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueFullyCompressed.DeadlineFullyCompressed
          override def deadlineUncompressed: Deadline.Uncompressed = ValueFullyCompressed.DeadlineUncompressed
        }
        object ValueFullyCompressed extends ValueFullyCompressed {
          object NoDeadline extends BaseEntryId(1750) with Deadline.NoDeadline with ValueFullyCompressed
          object DeadlineOneCompressed extends BaseEntryId(1751) with Deadline.OneCompressed with ValueFullyCompressed
          object DeadlineTwoCompressed extends BaseEntryId(1752) with Deadline.TwoCompressed with ValueFullyCompressed
          object DeadlineThreeCompressed extends BaseEntryId(1753) with Deadline.ThreeCompressed with ValueFullyCompressed
          object DeadlineFourCompressed extends BaseEntryId(1754) with Deadline.FourCompressed with ValueFullyCompressed
          object DeadlineFiveCompressed extends BaseEntryId(1755) with Deadline.FiveCompressed with ValueFullyCompressed
          object DeadlineSixCompressed extends BaseEntryId(1756) with Deadline.SixCompressed with ValueFullyCompressed
          object DeadlineSevenCompressed extends BaseEntryId(1757) with Deadline.SevenCompressed with ValueFullyCompressed
          object DeadlineFullyCompressed extends BaseEntryId(1758) with Deadline.FullyCompressed with ValueFullyCompressed
          object DeadlineUncompressed extends BaseEntryId(1759) with Deadline.Uncompressed with ValueFullyCompressed
        }
      }
    }

    trait KeyUncompressed extends Key.Uncompressed with FormatA {
      override def noTime: Time.NoTime = KeyUncompressed.NoTime
      override def timeFullyCompressed: Time.FullyCompressed = KeyUncompressed.TimeFullyCompressed
      override def timePartiallyCompressed: Time.PartiallyCompressed = KeyUncompressed.TimePartiallyCompressed
      override def timeUncompressed: Time.Uncompressed = KeyUncompressed.TimeUncompressed
    }
    object KeyUncompressed extends KeyUncompressed {

      trait TimePartiallyCompressed extends Time.PartiallyCompressed with KeyUncompressed {
        override def noValue: Value.NoValue = TimePartiallyCompressed.NoValue
        override def valueFullyCompressed: Value.FullyCompressed = TimePartiallyCompressed.ValueFullyCompressed
        override def valueUncompressed: Value.Uncompressed = TimePartiallyCompressed.ValueUncompressed
      }
      object TimePartiallyCompressed extends TimePartiallyCompressed {

        trait ValueUncompressed extends Value.Uncompressed with TimePartiallyCompressed {
          override def valueOffsetOneCompressed: ValueOffset.OneCompressed = ValueUncompressed.ValueOffsetOneCompressed
          override def valueOffsetTwoCompressed: ValueOffset.TwoCompressed = ValueUncompressed.ValueOffsetTwoCompressed
          override def valueOffsetThreeCompressed: ValueOffset.ThreeCompressed = ValueUncompressed.ValueOffsetThreeCompressed
          override def valueOffsetUncompressed: ValueOffset.Uncompressed = ValueUncompressed.ValueOffsetUncompressed
        }
        object ValueUncompressed extends ValueUncompressed {

          trait ValueOffsetOneCompressed extends ValueOffset.OneCompressed with ValueUncompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetOneCompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetOneCompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetOneCompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetOneCompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetOneCompressed.ValueLengthUncompressed
          }
          object ValueOffsetOneCompressed extends ValueOffsetOneCompressed {
            trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetOneCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthOneCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthOneCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthOneCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthOneCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthOneCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthOneCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthOneCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthOneCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthOneCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthOneCompressed.DeadlineUncompressed
            }

            object ValueLengthOneCompressed extends ValueLengthOneCompressed {
              object NoDeadline extends BaseEntryId(1760) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryId(1761) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1762) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1763) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryId(1764) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1765) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryId(1766) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1767) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1768) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryId(1769) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetOneCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthTwoCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthTwoCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthTwoCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthTwoCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthTwoCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthTwoCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthTwoCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthTwoCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthTwoCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthTwoCompressed.DeadlineUncompressed
            }

            object ValueLengthTwoCompressed extends ValueLengthTwoCompressed {
              object NoDeadline extends BaseEntryId(1770) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryId(1771) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1772) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1773) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryId(1774) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1775) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryId(1776) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1777) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1778) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryId(1779) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetOneCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthThreeCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthThreeCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthThreeCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthThreeCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthThreeCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthThreeCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthThreeCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthThreeCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthThreeCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthThreeCompressed.DeadlineUncompressed
            }

            object ValueLengthThreeCompressed extends ValueLengthThreeCompressed {
              object NoDeadline extends BaseEntryId(1780) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryId(1781) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1782) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1783) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryId(1784) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1785) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryId(1786) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1787) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1788) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryId(1789) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetOneCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthFullyCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthFullyCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthFullyCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthFullyCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthFullyCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthFullyCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthFullyCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthFullyCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthFullyCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthFullyCompressed.DeadlineUncompressed
            }

            object ValueLengthFullyCompressed extends ValueLengthFullyCompressed {
              object NoDeadline extends BaseEntryId(1790) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryId(1791) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1792) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1793) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryId(1794) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1795) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryId(1796) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1797) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1798) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryId(1799) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetOneCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthUncompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthUncompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthUncompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthUncompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthUncompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthUncompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthUncompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthUncompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthUncompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthUncompressed.DeadlineUncompressed
            }

            object ValueLengthUncompressed extends ValueLengthUncompressed {
              object NoDeadline extends BaseEntryId(1800) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryId(1801) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryId(1802) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryId(1803) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryId(1804) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryId(1805) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryId(1806) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryId(1807) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryId(1808) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryId(1809) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }

          trait ValueOffsetTwoCompressed extends ValueOffset.TwoCompressed with ValueUncompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetTwoCompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetTwoCompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetTwoCompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetTwoCompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetTwoCompressed.ValueLengthUncompressed
          }
          object ValueOffsetTwoCompressed extends ValueOffsetTwoCompressed {
            trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetTwoCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthOneCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthOneCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthOneCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthOneCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthOneCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthOneCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthOneCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthOneCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthOneCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthOneCompressed.DeadlineUncompressed
            }

            object ValueLengthOneCompressed extends ValueLengthOneCompressed {
              object NoDeadline extends BaseEntryId(1810) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryId(1811) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1812) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1813) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryId(1814) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1815) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryId(1816) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1817) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1818) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryId(1819) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetTwoCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthTwoCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthTwoCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthTwoCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthTwoCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthTwoCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthTwoCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthTwoCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthTwoCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthTwoCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthTwoCompressed.DeadlineUncompressed
            }

            object ValueLengthTwoCompressed extends ValueLengthTwoCompressed {
              object NoDeadline extends BaseEntryId(1820) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryId(1821) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1822) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1823) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryId(1824) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1825) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryId(1826) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1827) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1828) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryId(1829) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetTwoCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthThreeCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthThreeCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthThreeCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthThreeCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthThreeCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthThreeCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthThreeCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthThreeCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthThreeCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthThreeCompressed.DeadlineUncompressed
            }

            object ValueLengthThreeCompressed extends ValueLengthThreeCompressed {
              object NoDeadline extends BaseEntryId(1830) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryId(1831) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1832) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1833) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryId(1834) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1835) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryId(1836) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1837) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1838) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryId(1839) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetTwoCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthFullyCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthFullyCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthFullyCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthFullyCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthFullyCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthFullyCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthFullyCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthFullyCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthFullyCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthFullyCompressed.DeadlineUncompressed
            }

            object ValueLengthFullyCompressed extends ValueLengthFullyCompressed {
              object NoDeadline extends BaseEntryId(1840) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryId(1841) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1842) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1843) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryId(1844) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1845) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryId(1846) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1847) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1848) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryId(1849) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetTwoCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthUncompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthUncompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthUncompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthUncompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthUncompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthUncompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthUncompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthUncompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthUncompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthUncompressed.DeadlineUncompressed
            }

            object ValueLengthUncompressed extends ValueLengthUncompressed {
              object NoDeadline extends BaseEntryId(1850) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryId(1851) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryId(1852) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryId(1853) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryId(1854) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryId(1855) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryId(1856) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryId(1857) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryId(1858) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryId(1859) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }

          trait ValueOffsetThreeCompressed extends ValueOffset.ThreeCompressed with ValueUncompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetThreeCompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetThreeCompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetThreeCompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetThreeCompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetThreeCompressed.ValueLengthUncompressed
          }
          object ValueOffsetThreeCompressed extends ValueOffsetThreeCompressed {
            trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetThreeCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthOneCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthOneCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthOneCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthOneCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthOneCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthOneCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthOneCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthOneCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthOneCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthOneCompressed.DeadlineUncompressed
            }

            object ValueLengthOneCompressed extends ValueLengthOneCompressed {
              object NoDeadline extends BaseEntryId(1860) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryId(1861) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1862) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1863) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryId(1864) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1865) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryId(1866) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1867) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1868) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryId(1869) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetThreeCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthTwoCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthTwoCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthTwoCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthTwoCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthTwoCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthTwoCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthTwoCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthTwoCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthTwoCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthTwoCompressed.DeadlineUncompressed
            }

            object ValueLengthTwoCompressed extends ValueLengthTwoCompressed {
              object NoDeadline extends BaseEntryId(1870) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryId(1871) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1872) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1873) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryId(1874) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1875) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryId(1876) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1877) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1878) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryId(1879) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetThreeCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthThreeCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthThreeCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthThreeCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthThreeCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthThreeCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthThreeCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthThreeCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthThreeCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthThreeCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthThreeCompressed.DeadlineUncompressed
            }

            object ValueLengthThreeCompressed extends ValueLengthThreeCompressed {
              object NoDeadline extends BaseEntryId(1880) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryId(1881) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1882) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1883) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryId(1884) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1885) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryId(1886) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1887) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1888) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryId(1889) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetThreeCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthFullyCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthFullyCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthFullyCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthFullyCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthFullyCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthFullyCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthFullyCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthFullyCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthFullyCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthFullyCompressed.DeadlineUncompressed
            }

            object ValueLengthFullyCompressed extends ValueLengthFullyCompressed {
              object NoDeadline extends BaseEntryId(1890) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryId(1891) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1892) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1893) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryId(1894) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1895) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryId(1896) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1897) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1898) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryId(1899) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetThreeCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthUncompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthUncompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthUncompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthUncompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthUncompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthUncompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthUncompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthUncompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthUncompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthUncompressed.DeadlineUncompressed
            }

            object ValueLengthUncompressed extends ValueLengthUncompressed {
              object NoDeadline extends BaseEntryId(1900) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryId(1901) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryId(1902) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryId(1903) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryId(1904) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryId(1905) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryId(1906) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryId(1907) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryId(1908) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryId(1909) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }

          trait ValueOffsetUncompressed extends ValueOffset.Uncompressed with ValueUncompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetUncompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetUncompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetUncompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetUncompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetUncompressed.ValueLengthUncompressed
          }
          object ValueOffsetUncompressed extends ValueOffsetUncompressed {
            trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetUncompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthOneCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthOneCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthOneCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthOneCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthOneCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthOneCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthOneCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthOneCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthOneCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthOneCompressed.DeadlineUncompressed
            }

            object ValueLengthOneCompressed extends ValueLengthOneCompressed {
              object NoDeadline extends BaseEntryId(1910) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryId(1911) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1912) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1913) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryId(1914) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1915) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryId(1916) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1917) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1918) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryId(1919) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetUncompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthTwoCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthTwoCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthTwoCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthTwoCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthTwoCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthTwoCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthTwoCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthTwoCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthTwoCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthTwoCompressed.DeadlineUncompressed
            }

            object ValueLengthTwoCompressed extends ValueLengthTwoCompressed {
              object NoDeadline extends BaseEntryId(1920) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryId(1921) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1922) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1923) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryId(1924) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1925) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryId(1926) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1927) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1928) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryId(1929) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetUncompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthThreeCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthThreeCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthThreeCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthThreeCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthThreeCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthThreeCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthThreeCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthThreeCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthThreeCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthThreeCompressed.DeadlineUncompressed
            }

            object ValueLengthThreeCompressed extends ValueLengthThreeCompressed {
              object NoDeadline extends BaseEntryId(1930) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryId(1931) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1932) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1933) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryId(1934) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1935) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryId(1936) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1937) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1938) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryId(1939) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetUncompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthFullyCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthFullyCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthFullyCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthFullyCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthFullyCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthFullyCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthFullyCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthFullyCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthFullyCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthFullyCompressed.DeadlineUncompressed
            }

            object ValueLengthFullyCompressed extends ValueLengthFullyCompressed {
              object NoDeadline extends BaseEntryId(1940) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryId(1941) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1942) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1943) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryId(1944) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1945) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryId(1946) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1947) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1948) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryId(1949) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetUncompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthUncompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthUncompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthUncompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthUncompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthUncompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthUncompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthUncompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthUncompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthUncompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthUncompressed.DeadlineUncompressed
            }

            object ValueLengthUncompressed extends ValueLengthUncompressed {
              object NoDeadline extends BaseEntryId(1950) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryId(1951) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryId(1952) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryId(1953) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryId(1954) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryId(1955) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryId(1956) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryId(1957) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryId(1958) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryId(1959) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }
        }

        trait NoValue extends Value.NoValue with TimePartiallyCompressed {
          override def noDeadline: Deadline.NoDeadline = NoValue.NoDeadline
          override def deadlineOneCompressed: Deadline.OneCompressed = NoValue.DeadlineOneCompressed
          override def deadlineTwoCompressed: Deadline.TwoCompressed = NoValue.DeadlineTwoCompressed
          override def deadlineThreeCompressed: Deadline.ThreeCompressed = NoValue.DeadlineThreeCompressed
          override def deadlineFourCompressed: Deadline.FourCompressed = NoValue.DeadlineFourCompressed
          override def deadlineFiveCompressed: Deadline.FiveCompressed = NoValue.DeadlineFiveCompressed
          override def deadlineSixCompressed: Deadline.SixCompressed = NoValue.DeadlineSixCompressed
          override def deadlineSevenCompressed: Deadline.SevenCompressed = NoValue.DeadlineSevenCompressed
          override def deadlineFullyCompressed: Deadline.FullyCompressed = NoValue.DeadlineFullyCompressed
          override def deadlineUncompressed: Deadline.Uncompressed = NoValue.DeadlineUncompressed
        }
        object NoValue extends NoValue {
          object NoDeadline extends BaseEntryId(1960) with Deadline.NoDeadline with NoValue
          object DeadlineOneCompressed extends BaseEntryId(1961) with Deadline.OneCompressed with NoValue
          object DeadlineTwoCompressed extends BaseEntryId(1962) with Deadline.TwoCompressed with NoValue
          object DeadlineThreeCompressed extends BaseEntryId(1963) with Deadline.ThreeCompressed with NoValue
          object DeadlineFourCompressed extends BaseEntryId(1964) with Deadline.FourCompressed with NoValue
          object DeadlineFiveCompressed extends BaseEntryId(1965) with Deadline.FiveCompressed with NoValue
          object DeadlineSixCompressed extends BaseEntryId(1966) with Deadline.SixCompressed with NoValue
          object DeadlineSevenCompressed extends BaseEntryId(1967) with Deadline.SevenCompressed with NoValue
          object DeadlineFullyCompressed extends BaseEntryId(1968) with Deadline.FullyCompressed with NoValue
          object DeadlineUncompressed extends BaseEntryId(1969) with Deadline.Uncompressed with NoValue
        }

        trait ValueFullyCompressed extends Value.FullyCompressed with TimePartiallyCompressed {
          override def noDeadline: Deadline.NoDeadline = ValueFullyCompressed.NoDeadline
          override def deadlineOneCompressed: Deadline.OneCompressed = ValueFullyCompressed.DeadlineOneCompressed
          override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueFullyCompressed.DeadlineTwoCompressed
          override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueFullyCompressed.DeadlineThreeCompressed
          override def deadlineFourCompressed: Deadline.FourCompressed = ValueFullyCompressed.DeadlineFourCompressed
          override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueFullyCompressed.DeadlineFiveCompressed
          override def deadlineSixCompressed: Deadline.SixCompressed = ValueFullyCompressed.DeadlineSixCompressed
          override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueFullyCompressed.DeadlineSevenCompressed
          override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueFullyCompressed.DeadlineFullyCompressed
          override def deadlineUncompressed: Deadline.Uncompressed = ValueFullyCompressed.DeadlineUncompressed
        }
        object ValueFullyCompressed extends ValueFullyCompressed {
          object NoDeadline extends BaseEntryId(1970) with Deadline.NoDeadline with ValueFullyCompressed
          object DeadlineOneCompressed extends BaseEntryId(1971) with Deadline.OneCompressed with ValueFullyCompressed
          object DeadlineTwoCompressed extends BaseEntryId(1972) with Deadline.TwoCompressed with ValueFullyCompressed
          object DeadlineThreeCompressed extends BaseEntryId(1973) with Deadline.ThreeCompressed with ValueFullyCompressed
          object DeadlineFourCompressed extends BaseEntryId(1974) with Deadline.FourCompressed with ValueFullyCompressed
          object DeadlineFiveCompressed extends BaseEntryId(1975) with Deadline.FiveCompressed with ValueFullyCompressed
          object DeadlineSixCompressed extends BaseEntryId(1976) with Deadline.SixCompressed with ValueFullyCompressed
          object DeadlineSevenCompressed extends BaseEntryId(1977) with Deadline.SevenCompressed with ValueFullyCompressed
          object DeadlineFullyCompressed extends BaseEntryId(1978) with Deadline.FullyCompressed with ValueFullyCompressed
          object DeadlineUncompressed extends BaseEntryId(1979) with Deadline.Uncompressed with ValueFullyCompressed
        }
      }

      trait TimeFullyCompressed extends Time.FullyCompressed with KeyUncompressed {
        override def noValue: Value.NoValue = TimeFullyCompressed.NoValue
        override def valueFullyCompressed: Value.FullyCompressed = TimeFullyCompressed.ValueFullyCompressed
        override def valueUncompressed: Value.Uncompressed = TimeFullyCompressed.ValueUncompressed
      }
      object TimeFullyCompressed extends TimeFullyCompressed {

        trait ValueUncompressed extends Value.Uncompressed with TimeFullyCompressed {
          override def valueOffsetOneCompressed: ValueOffset.OneCompressed = ValueUncompressed.ValueOffsetOneCompressed
          override def valueOffsetTwoCompressed: ValueOffset.TwoCompressed = ValueUncompressed.ValueOffsetTwoCompressed
          override def valueOffsetThreeCompressed: ValueOffset.ThreeCompressed = ValueUncompressed.ValueOffsetThreeCompressed
          override def valueOffsetUncompressed: ValueOffset.Uncompressed = ValueUncompressed.ValueOffsetUncompressed
        }
        object ValueUncompressed extends ValueUncompressed {

          trait ValueOffsetOneCompressed extends ValueOffset.OneCompressed with ValueUncompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetOneCompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetOneCompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetOneCompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetOneCompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetOneCompressed.ValueLengthUncompressed
          }
          object ValueOffsetOneCompressed extends ValueOffsetOneCompressed {
            trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetOneCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthOneCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthOneCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthOneCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthOneCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthOneCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthOneCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthOneCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthOneCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthOneCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthOneCompressed.DeadlineUncompressed
            }

            object ValueLengthOneCompressed extends ValueLengthOneCompressed {
              object NoDeadline extends BaseEntryId(1980) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryId(1981) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1982) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1983) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryId(1984) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1985) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryId(1986) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1987) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1988) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryId(1989) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetOneCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthTwoCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthTwoCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthTwoCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthTwoCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthTwoCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthTwoCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthTwoCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthTwoCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthTwoCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthTwoCompressed.DeadlineUncompressed
            }

            object ValueLengthTwoCompressed extends ValueLengthTwoCompressed {
              object NoDeadline extends BaseEntryId(1990) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryId(1991) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryId(1992) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryId(1993) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryId(1994) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryId(1995) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryId(1996) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryId(1997) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryId(1998) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryId(1999) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetOneCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthThreeCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthThreeCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthThreeCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthThreeCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthThreeCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthThreeCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthThreeCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthThreeCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthThreeCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthThreeCompressed.DeadlineUncompressed
            }

            object ValueLengthThreeCompressed extends ValueLengthThreeCompressed {
              object NoDeadline extends BaseEntryId(2000) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryId(2001) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryId(2002) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryId(2003) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryId(2004) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryId(2005) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryId(2006) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryId(2007) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryId(2008) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryId(2009) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetOneCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthFullyCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthFullyCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthFullyCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthFullyCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthFullyCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthFullyCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthFullyCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthFullyCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthFullyCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthFullyCompressed.DeadlineUncompressed
            }

            object ValueLengthFullyCompressed extends ValueLengthFullyCompressed {
              object NoDeadline extends BaseEntryId(2010) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryId(2011) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryId(2012) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryId(2013) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryId(2014) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryId(2015) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryId(2016) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryId(2017) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryId(2018) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryId(2019) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetOneCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthUncompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthUncompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthUncompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthUncompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthUncompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthUncompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthUncompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthUncompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthUncompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthUncompressed.DeadlineUncompressed
            }

            object ValueLengthUncompressed extends ValueLengthUncompressed {
              object NoDeadline extends BaseEntryId(2020) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryId(2021) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryId(2022) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryId(2023) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryId(2024) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryId(2025) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryId(2026) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryId(2027) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryId(2028) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryId(2029) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }

          trait ValueOffsetTwoCompressed extends ValueOffset.TwoCompressed with ValueUncompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetTwoCompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetTwoCompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetTwoCompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetTwoCompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetTwoCompressed.ValueLengthUncompressed
          }
          object ValueOffsetTwoCompressed extends ValueOffsetTwoCompressed {
            trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetTwoCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthOneCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthOneCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthOneCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthOneCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthOneCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthOneCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthOneCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthOneCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthOneCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthOneCompressed.DeadlineUncompressed
            }

            object ValueLengthOneCompressed extends ValueLengthOneCompressed {
              object NoDeadline extends BaseEntryId(2030) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryId(2031) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryId(2032) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryId(2033) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryId(2034) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryId(2035) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryId(2036) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryId(2037) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryId(2038) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryId(2039) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetTwoCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthTwoCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthTwoCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthTwoCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthTwoCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthTwoCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthTwoCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthTwoCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthTwoCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthTwoCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthTwoCompressed.DeadlineUncompressed
            }

            object ValueLengthTwoCompressed extends ValueLengthTwoCompressed {
              object NoDeadline extends BaseEntryId(2040) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryId(2041) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryId(2042) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryId(2043) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryId(2044) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryId(2045) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryId(2046) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryId(2047) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryId(2048) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryId(2049) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetTwoCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthThreeCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthThreeCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthThreeCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthThreeCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthThreeCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthThreeCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthThreeCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthThreeCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthThreeCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthThreeCompressed.DeadlineUncompressed
            }

            object ValueLengthThreeCompressed extends ValueLengthThreeCompressed {
              object NoDeadline extends BaseEntryId(2050) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryId(2051) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryId(2052) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryId(2053) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryId(2054) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryId(2055) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryId(2056) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryId(2057) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryId(2058) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryId(2059) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetTwoCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthFullyCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthFullyCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthFullyCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthFullyCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthFullyCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthFullyCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthFullyCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthFullyCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthFullyCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthFullyCompressed.DeadlineUncompressed
            }

            object ValueLengthFullyCompressed extends ValueLengthFullyCompressed {
              object NoDeadline extends BaseEntryId(2060) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryId(2061) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryId(2062) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryId(2063) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryId(2064) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryId(2065) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryId(2066) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryId(2067) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryId(2068) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryId(2069) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetTwoCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthUncompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthUncompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthUncompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthUncompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthUncompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthUncompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthUncompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthUncompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthUncompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthUncompressed.DeadlineUncompressed
            }

            object ValueLengthUncompressed extends ValueLengthUncompressed {
              object NoDeadline extends BaseEntryId(2070) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryId(2071) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryId(2072) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryId(2073) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryId(2074) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryId(2075) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryId(2076) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryId(2077) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryId(2078) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryId(2079) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }

          trait ValueOffsetThreeCompressed extends ValueOffset.ThreeCompressed with ValueUncompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetThreeCompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetThreeCompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetThreeCompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetThreeCompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetThreeCompressed.ValueLengthUncompressed
          }
          object ValueOffsetThreeCompressed extends ValueOffsetThreeCompressed {
            trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetThreeCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthOneCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthOneCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthOneCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthOneCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthOneCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthOneCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthOneCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthOneCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthOneCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthOneCompressed.DeadlineUncompressed
            }

            object ValueLengthOneCompressed extends ValueLengthOneCompressed {
              object NoDeadline extends BaseEntryId(2080) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryId(2081) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryId(2082) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryId(2083) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryId(2084) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryId(2085) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryId(2086) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryId(2087) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryId(2088) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryId(2089) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetThreeCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthTwoCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthTwoCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthTwoCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthTwoCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthTwoCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthTwoCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthTwoCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthTwoCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthTwoCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthTwoCompressed.DeadlineUncompressed
            }

            object ValueLengthTwoCompressed extends ValueLengthTwoCompressed {
              object NoDeadline extends BaseEntryId(2090) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryId(2091) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryId(2092) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryId(2093) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryId(2094) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryId(2095) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryId(2096) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryId(2097) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryId(2098) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryId(2099) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetThreeCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthThreeCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthThreeCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthThreeCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthThreeCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthThreeCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthThreeCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthThreeCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthThreeCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthThreeCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthThreeCompressed.DeadlineUncompressed
            }

            object ValueLengthThreeCompressed extends ValueLengthThreeCompressed {
              object NoDeadline extends BaseEntryId(2100) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryId(2101) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryId(2102) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryId(2103) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryId(2104) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryId(2105) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryId(2106) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryId(2107) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryId(2108) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryId(2109) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetThreeCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthFullyCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthFullyCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthFullyCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthFullyCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthFullyCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthFullyCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthFullyCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthFullyCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthFullyCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthFullyCompressed.DeadlineUncompressed
            }

            object ValueLengthFullyCompressed extends ValueLengthFullyCompressed {
              object NoDeadline extends BaseEntryId(2110) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryId(2111) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryId(2112) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryId(2113) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryId(2114) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryId(2115) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryId(2116) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryId(2117) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryId(2118) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryId(2119) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetThreeCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthUncompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthUncompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthUncompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthUncompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthUncompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthUncompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthUncompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthUncompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthUncompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthUncompressed.DeadlineUncompressed
            }

            object ValueLengthUncompressed extends ValueLengthUncompressed {
              object NoDeadline extends BaseEntryId(2120) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryId(2121) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryId(2122) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryId(2123) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryId(2124) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryId(2125) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryId(2126) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryId(2127) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryId(2128) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryId(2129) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }

          trait ValueOffsetUncompressed extends ValueOffset.Uncompressed with ValueUncompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetUncompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetUncompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetUncompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetUncompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetUncompressed.ValueLengthUncompressed
          }
          object ValueOffsetUncompressed extends ValueOffsetUncompressed {
            trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetUncompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthOneCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthOneCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthOneCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthOneCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthOneCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthOneCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthOneCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthOneCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthOneCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthOneCompressed.DeadlineUncompressed
            }

            object ValueLengthOneCompressed extends ValueLengthOneCompressed {
              object NoDeadline extends BaseEntryId(2130) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryId(2131) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryId(2132) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryId(2133) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryId(2134) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryId(2135) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryId(2136) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryId(2137) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryId(2138) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryId(2139) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetUncompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthTwoCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthTwoCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthTwoCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthTwoCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthTwoCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthTwoCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthTwoCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthTwoCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthTwoCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthTwoCompressed.DeadlineUncompressed
            }

            object ValueLengthTwoCompressed extends ValueLengthTwoCompressed {
              object NoDeadline extends BaseEntryId(2140) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryId(2141) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryId(2142) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryId(2143) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryId(2144) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryId(2145) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryId(2146) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryId(2147) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryId(2148) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryId(2149) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetUncompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthThreeCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthThreeCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthThreeCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthThreeCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthThreeCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthThreeCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthThreeCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthThreeCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthThreeCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthThreeCompressed.DeadlineUncompressed
            }

            object ValueLengthThreeCompressed extends ValueLengthThreeCompressed {
              object NoDeadline extends BaseEntryId(2150) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryId(2151) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryId(2152) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryId(2153) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryId(2154) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryId(2155) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryId(2156) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryId(2157) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryId(2158) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryId(2159) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetUncompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthFullyCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthFullyCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthFullyCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthFullyCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthFullyCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthFullyCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthFullyCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthFullyCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthFullyCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthFullyCompressed.DeadlineUncompressed
            }

            object ValueLengthFullyCompressed extends ValueLengthFullyCompressed {
              object NoDeadline extends BaseEntryId(2160) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryId(2161) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryId(2162) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryId(2163) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryId(2164) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryId(2165) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryId(2166) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryId(2167) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryId(2168) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryId(2169) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetUncompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthUncompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthUncompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthUncompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthUncompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthUncompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthUncompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthUncompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthUncompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthUncompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthUncompressed.DeadlineUncompressed
            }

            object ValueLengthUncompressed extends ValueLengthUncompressed {
              object NoDeadline extends BaseEntryId(2170) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryId(2171) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryId(2172) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryId(2173) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryId(2174) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryId(2175) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryId(2176) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryId(2177) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryId(2178) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryId(2179) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }
        }

        trait NoValue extends Value.NoValue with TimeFullyCompressed {
          override def noDeadline: Deadline.NoDeadline = NoValue.NoDeadline
          override def deadlineOneCompressed: Deadline.OneCompressed = NoValue.DeadlineOneCompressed
          override def deadlineTwoCompressed: Deadline.TwoCompressed = NoValue.DeadlineTwoCompressed
          override def deadlineThreeCompressed: Deadline.ThreeCompressed = NoValue.DeadlineThreeCompressed
          override def deadlineFourCompressed: Deadline.FourCompressed = NoValue.DeadlineFourCompressed
          override def deadlineFiveCompressed: Deadline.FiveCompressed = NoValue.DeadlineFiveCompressed
          override def deadlineSixCompressed: Deadline.SixCompressed = NoValue.DeadlineSixCompressed
          override def deadlineSevenCompressed: Deadline.SevenCompressed = NoValue.DeadlineSevenCompressed
          override def deadlineFullyCompressed: Deadline.FullyCompressed = NoValue.DeadlineFullyCompressed
          override def deadlineUncompressed: Deadline.Uncompressed = NoValue.DeadlineUncompressed
        }
        object NoValue extends NoValue {
          object NoDeadline extends BaseEntryId(2180) with Deadline.NoDeadline with NoValue
          object DeadlineOneCompressed extends BaseEntryId(2181) with Deadline.OneCompressed with NoValue
          object DeadlineTwoCompressed extends BaseEntryId(2182) with Deadline.TwoCompressed with NoValue
          object DeadlineThreeCompressed extends BaseEntryId(2183) with Deadline.ThreeCompressed with NoValue
          object DeadlineFourCompressed extends BaseEntryId(2184) with Deadline.FourCompressed with NoValue
          object DeadlineFiveCompressed extends BaseEntryId(2185) with Deadline.FiveCompressed with NoValue
          object DeadlineSixCompressed extends BaseEntryId(2186) with Deadline.SixCompressed with NoValue
          object DeadlineSevenCompressed extends BaseEntryId(2187) with Deadline.SevenCompressed with NoValue
          object DeadlineFullyCompressed extends BaseEntryId(2188) with Deadline.FullyCompressed with NoValue
          object DeadlineUncompressed extends BaseEntryId(2189) with Deadline.Uncompressed with NoValue
        }

        trait ValueFullyCompressed extends Value.FullyCompressed with TimeFullyCompressed {
          override def noDeadline: Deadline.NoDeadline = ValueFullyCompressed.NoDeadline
          override def deadlineOneCompressed: Deadline.OneCompressed = ValueFullyCompressed.DeadlineOneCompressed
          override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueFullyCompressed.DeadlineTwoCompressed
          override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueFullyCompressed.DeadlineThreeCompressed
          override def deadlineFourCompressed: Deadline.FourCompressed = ValueFullyCompressed.DeadlineFourCompressed
          override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueFullyCompressed.DeadlineFiveCompressed
          override def deadlineSixCompressed: Deadline.SixCompressed = ValueFullyCompressed.DeadlineSixCompressed
          override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueFullyCompressed.DeadlineSevenCompressed
          override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueFullyCompressed.DeadlineFullyCompressed
          override def deadlineUncompressed: Deadline.Uncompressed = ValueFullyCompressed.DeadlineUncompressed
        }
        object ValueFullyCompressed extends ValueFullyCompressed {
          object NoDeadline extends BaseEntryId(2190) with Deadline.NoDeadline with ValueFullyCompressed
          object DeadlineOneCompressed extends BaseEntryId(2191) with Deadline.OneCompressed with ValueFullyCompressed
          object DeadlineTwoCompressed extends BaseEntryId(2192) with Deadline.TwoCompressed with ValueFullyCompressed
          object DeadlineThreeCompressed extends BaseEntryId(2193) with Deadline.ThreeCompressed with ValueFullyCompressed
          object DeadlineFourCompressed extends BaseEntryId(2194) with Deadline.FourCompressed with ValueFullyCompressed
          object DeadlineFiveCompressed extends BaseEntryId(2195) with Deadline.FiveCompressed with ValueFullyCompressed
          object DeadlineSixCompressed extends BaseEntryId(2196) with Deadline.SixCompressed with ValueFullyCompressed
          object DeadlineSevenCompressed extends BaseEntryId(2197) with Deadline.SevenCompressed with ValueFullyCompressed
          object DeadlineFullyCompressed extends BaseEntryId(2198) with Deadline.FullyCompressed with ValueFullyCompressed
          object DeadlineUncompressed extends BaseEntryId(2199) with Deadline.Uncompressed with ValueFullyCompressed
        }
      }

      trait TimeUncompressed extends Time.Uncompressed with KeyUncompressed {
        override def noValue: Value.NoValue = TimeUncompressed.NoValue
        override def valueFullyCompressed: Value.FullyCompressed = TimeUncompressed.ValueFullyCompressed
        override def valueUncompressed: Value.Uncompressed = TimeUncompressed.ValueUncompressed
      }
      object TimeUncompressed extends TimeUncompressed {

        trait ValueUncompressed extends Value.Uncompressed with TimeUncompressed {
          override def valueOffsetOneCompressed: ValueOffset.OneCompressed = ValueUncompressed.ValueOffsetOneCompressed
          override def valueOffsetTwoCompressed: ValueOffset.TwoCompressed = ValueUncompressed.ValueOffsetTwoCompressed
          override def valueOffsetThreeCompressed: ValueOffset.ThreeCompressed = ValueUncompressed.ValueOffsetThreeCompressed
          override def valueOffsetUncompressed: ValueOffset.Uncompressed = ValueUncompressed.ValueOffsetUncompressed
        }
        object ValueUncompressed extends ValueUncompressed {

          trait ValueOffsetOneCompressed extends ValueOffset.OneCompressed with ValueUncompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetOneCompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetOneCompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetOneCompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetOneCompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetOneCompressed.ValueLengthUncompressed
          }
          object ValueOffsetOneCompressed extends ValueOffsetOneCompressed {
            trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetOneCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthOneCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthOneCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthOneCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthOneCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthOneCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthOneCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthOneCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthOneCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthOneCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthOneCompressed.DeadlineUncompressed
            }

            object ValueLengthOneCompressed extends ValueLengthOneCompressed {
              object NoDeadline extends BaseEntryId(2200) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryId(2201) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryId(2202) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryId(2203) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryId(2204) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryId(2205) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryId(2206) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryId(2207) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryId(2208) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryId(2209) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetOneCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthTwoCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthTwoCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthTwoCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthTwoCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthTwoCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthTwoCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthTwoCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthTwoCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthTwoCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthTwoCompressed.DeadlineUncompressed
            }

            object ValueLengthTwoCompressed extends ValueLengthTwoCompressed {
              object NoDeadline extends BaseEntryId(2210) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryId(2211) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryId(2212) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryId(2213) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryId(2214) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryId(2215) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryId(2216) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryId(2217) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryId(2218) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryId(2219) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetOneCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthThreeCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthThreeCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthThreeCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthThreeCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthThreeCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthThreeCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthThreeCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthThreeCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthThreeCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthThreeCompressed.DeadlineUncompressed
            }

            object ValueLengthThreeCompressed extends ValueLengthThreeCompressed {
              object NoDeadline extends BaseEntryId(2220) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryId(2221) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryId(2222) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryId(2223) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryId(2224) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryId(2225) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryId(2226) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryId(2227) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryId(2228) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryId(2229) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetOneCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthFullyCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthFullyCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthFullyCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthFullyCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthFullyCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthFullyCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthFullyCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthFullyCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthFullyCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthFullyCompressed.DeadlineUncompressed
            }

            object ValueLengthFullyCompressed extends ValueLengthFullyCompressed {
              object NoDeadline extends BaseEntryId(2230) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryId(2231) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryId(2232) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryId(2233) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryId(2234) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryId(2235) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryId(2236) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryId(2237) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryId(2238) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryId(2239) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetOneCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthUncompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthUncompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthUncompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthUncompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthUncompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthUncompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthUncompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthUncompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthUncompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthUncompressed.DeadlineUncompressed
            }

            object ValueLengthUncompressed extends ValueLengthUncompressed {
              object NoDeadline extends BaseEntryId(2240) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryId(2241) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryId(2242) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryId(2243) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryId(2244) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryId(2245) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryId(2246) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryId(2247) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryId(2248) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryId(2249) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }

          trait ValueOffsetTwoCompressed extends ValueOffset.TwoCompressed with ValueUncompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetTwoCompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetTwoCompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetTwoCompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetTwoCompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetTwoCompressed.ValueLengthUncompressed
          }
          object ValueOffsetTwoCompressed extends ValueOffsetTwoCompressed {
            trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetTwoCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthOneCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthOneCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthOneCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthOneCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthOneCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthOneCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthOneCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthOneCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthOneCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthOneCompressed.DeadlineUncompressed
            }

            object ValueLengthOneCompressed extends ValueLengthOneCompressed {
              object NoDeadline extends BaseEntryId(2250) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryId(2251) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryId(2252) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryId(2253) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryId(2254) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryId(2255) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryId(2256) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryId(2257) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryId(2258) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryId(2259) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetTwoCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthTwoCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthTwoCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthTwoCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthTwoCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthTwoCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthTwoCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthTwoCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthTwoCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthTwoCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthTwoCompressed.DeadlineUncompressed
            }

            object ValueLengthTwoCompressed extends ValueLengthTwoCompressed {
              object NoDeadline extends BaseEntryId(2260) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryId(2261) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryId(2262) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryId(2263) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryId(2264) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryId(2265) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryId(2266) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryId(2267) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryId(2268) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryId(2269) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetTwoCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthThreeCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthThreeCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthThreeCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthThreeCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthThreeCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthThreeCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthThreeCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthThreeCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthThreeCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthThreeCompressed.DeadlineUncompressed
            }

            object ValueLengthThreeCompressed extends ValueLengthThreeCompressed {
              object NoDeadline extends BaseEntryId(2270) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryId(2271) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryId(2272) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryId(2273) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryId(2274) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryId(2275) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryId(2276) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryId(2277) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryId(2278) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryId(2279) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetTwoCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthFullyCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthFullyCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthFullyCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthFullyCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthFullyCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthFullyCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthFullyCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthFullyCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthFullyCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthFullyCompressed.DeadlineUncompressed
            }

            object ValueLengthFullyCompressed extends ValueLengthFullyCompressed {
              object NoDeadline extends BaseEntryId(2280) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryId(2281) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryId(2282) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryId(2283) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryId(2284) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryId(2285) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryId(2286) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryId(2287) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryId(2288) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryId(2289) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetTwoCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthUncompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthUncompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthUncompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthUncompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthUncompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthUncompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthUncompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthUncompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthUncompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthUncompressed.DeadlineUncompressed
            }

            object ValueLengthUncompressed extends ValueLengthUncompressed {
              object NoDeadline extends BaseEntryId(2290) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryId(2291) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryId(2292) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryId(2293) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryId(2294) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryId(2295) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryId(2296) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryId(2297) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryId(2298) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryId(2299) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }

          trait ValueOffsetThreeCompressed extends ValueOffset.ThreeCompressed with ValueUncompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetThreeCompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetThreeCompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetThreeCompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetThreeCompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetThreeCompressed.ValueLengthUncompressed
          }
          object ValueOffsetThreeCompressed extends ValueOffsetThreeCompressed {
            trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetThreeCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthOneCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthOneCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthOneCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthOneCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthOneCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthOneCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthOneCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthOneCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthOneCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthOneCompressed.DeadlineUncompressed
            }

            object ValueLengthOneCompressed extends ValueLengthOneCompressed {
              object NoDeadline extends BaseEntryId(2300) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryId(2301) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryId(2302) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryId(2303) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryId(2304) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryId(2305) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryId(2306) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryId(2307) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryId(2308) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryId(2309) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetThreeCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthTwoCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthTwoCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthTwoCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthTwoCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthTwoCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthTwoCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthTwoCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthTwoCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthTwoCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthTwoCompressed.DeadlineUncompressed
            }

            object ValueLengthTwoCompressed extends ValueLengthTwoCompressed {
              object NoDeadline extends BaseEntryId(2310) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryId(2311) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryId(2312) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryId(2313) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryId(2314) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryId(2315) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryId(2316) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryId(2317) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryId(2318) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryId(2319) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetThreeCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthThreeCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthThreeCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthThreeCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthThreeCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthThreeCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthThreeCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthThreeCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthThreeCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthThreeCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthThreeCompressed.DeadlineUncompressed
            }

            object ValueLengthThreeCompressed extends ValueLengthThreeCompressed {
              object NoDeadline extends BaseEntryId(2320) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryId(2321) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryId(2322) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryId(2323) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryId(2324) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryId(2325) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryId(2326) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryId(2327) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryId(2328) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryId(2329) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetThreeCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthFullyCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthFullyCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthFullyCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthFullyCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthFullyCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthFullyCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthFullyCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthFullyCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthFullyCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthFullyCompressed.DeadlineUncompressed
            }

            object ValueLengthFullyCompressed extends ValueLengthFullyCompressed {
              object NoDeadline extends BaseEntryId(2330) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryId(2331) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryId(2332) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryId(2333) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryId(2334) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryId(2335) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryId(2336) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryId(2337) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryId(2338) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryId(2339) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetThreeCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthUncompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthUncompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthUncompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthUncompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthUncompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthUncompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthUncompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthUncompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthUncompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthUncompressed.DeadlineUncompressed
            }

            object ValueLengthUncompressed extends ValueLengthUncompressed {
              object NoDeadline extends BaseEntryId(2340) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryId(2341) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryId(2342) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryId(2343) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryId(2344) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryId(2345) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryId(2346) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryId(2347) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryId(2348) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryId(2349) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }

          trait ValueOffsetUncompressed extends ValueOffset.Uncompressed with ValueUncompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetUncompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetUncompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetUncompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetUncompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetUncompressed.ValueLengthUncompressed
          }
          object ValueOffsetUncompressed extends ValueOffsetUncompressed {
            trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetUncompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthOneCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthOneCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthOneCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthOneCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthOneCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthOneCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthOneCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthOneCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthOneCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthOneCompressed.DeadlineUncompressed
            }

            object ValueLengthOneCompressed extends ValueLengthOneCompressed {
              object NoDeadline extends BaseEntryId(2350) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryId(2351) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryId(2352) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryId(2353) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryId(2354) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryId(2355) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryId(2356) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryId(2357) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryId(2358) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryId(2359) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetUncompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthTwoCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthTwoCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthTwoCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthTwoCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthTwoCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthTwoCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthTwoCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthTwoCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthTwoCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthTwoCompressed.DeadlineUncompressed
            }

            object ValueLengthTwoCompressed extends ValueLengthTwoCompressed {
              object NoDeadline extends BaseEntryId(2360) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryId(2361) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryId(2362) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryId(2363) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryId(2364) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryId(2365) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryId(2366) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryId(2367) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryId(2368) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryId(2369) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetUncompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthThreeCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthThreeCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthThreeCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthThreeCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthThreeCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthThreeCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthThreeCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthThreeCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthThreeCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthThreeCompressed.DeadlineUncompressed
            }

            object ValueLengthThreeCompressed extends ValueLengthThreeCompressed {
              object NoDeadline extends BaseEntryId(2370) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryId(2371) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryId(2372) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryId(2373) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryId(2374) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryId(2375) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryId(2376) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryId(2377) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryId(2378) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryId(2379) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetUncompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthFullyCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthFullyCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthFullyCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthFullyCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthFullyCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthFullyCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthFullyCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthFullyCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthFullyCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthFullyCompressed.DeadlineUncompressed
            }

            object ValueLengthFullyCompressed extends ValueLengthFullyCompressed {
              object NoDeadline extends BaseEntryId(2380) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryId(2381) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryId(2382) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryId(2383) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryId(2384) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryId(2385) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryId(2386) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryId(2387) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryId(2388) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryId(2389) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetUncompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthUncompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthUncompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthUncompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthUncompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthUncompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthUncompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthUncompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthUncompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthUncompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthUncompressed.DeadlineUncompressed
            }

            object ValueLengthUncompressed extends ValueLengthUncompressed {
              object NoDeadline extends BaseEntryId(2390) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryId(2391) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryId(2392) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryId(2393) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryId(2394) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryId(2395) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryId(2396) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryId(2397) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryId(2398) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryId(2399) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }
        }

        trait NoValue extends Value.NoValue with TimeUncompressed {
          override def noDeadline: Deadline.NoDeadline = NoValue.NoDeadline
          override def deadlineOneCompressed: Deadline.OneCompressed = NoValue.DeadlineOneCompressed
          override def deadlineTwoCompressed: Deadline.TwoCompressed = NoValue.DeadlineTwoCompressed
          override def deadlineThreeCompressed: Deadline.ThreeCompressed = NoValue.DeadlineThreeCompressed
          override def deadlineFourCompressed: Deadline.FourCompressed = NoValue.DeadlineFourCompressed
          override def deadlineFiveCompressed: Deadline.FiveCompressed = NoValue.DeadlineFiveCompressed
          override def deadlineSixCompressed: Deadline.SixCompressed = NoValue.DeadlineSixCompressed
          override def deadlineSevenCompressed: Deadline.SevenCompressed = NoValue.DeadlineSevenCompressed
          override def deadlineFullyCompressed: Deadline.FullyCompressed = NoValue.DeadlineFullyCompressed
          override def deadlineUncompressed: Deadline.Uncompressed = NoValue.DeadlineUncompressed
        }
        object NoValue extends NoValue {
          object NoDeadline extends BaseEntryId(2400) with Deadline.NoDeadline with NoValue
          object DeadlineOneCompressed extends BaseEntryId(2401) with Deadline.OneCompressed with NoValue
          object DeadlineTwoCompressed extends BaseEntryId(2402) with Deadline.TwoCompressed with NoValue
          object DeadlineThreeCompressed extends BaseEntryId(2403) with Deadline.ThreeCompressed with NoValue
          object DeadlineFourCompressed extends BaseEntryId(2404) with Deadline.FourCompressed with NoValue
          object DeadlineFiveCompressed extends BaseEntryId(2405) with Deadline.FiveCompressed with NoValue
          object DeadlineSixCompressed extends BaseEntryId(2406) with Deadline.SixCompressed with NoValue
          object DeadlineSevenCompressed extends BaseEntryId(2407) with Deadline.SevenCompressed with NoValue
          object DeadlineFullyCompressed extends BaseEntryId(2408) with Deadline.FullyCompressed with NoValue
          object DeadlineUncompressed extends BaseEntryId(2409) with Deadline.Uncompressed with NoValue
        }

        trait ValueFullyCompressed extends Value.FullyCompressed with TimeUncompressed {
          override def noDeadline: Deadline.NoDeadline = ValueFullyCompressed.NoDeadline
          override def deadlineOneCompressed: Deadline.OneCompressed = ValueFullyCompressed.DeadlineOneCompressed
          override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueFullyCompressed.DeadlineTwoCompressed
          override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueFullyCompressed.DeadlineThreeCompressed
          override def deadlineFourCompressed: Deadline.FourCompressed = ValueFullyCompressed.DeadlineFourCompressed
          override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueFullyCompressed.DeadlineFiveCompressed
          override def deadlineSixCompressed: Deadline.SixCompressed = ValueFullyCompressed.DeadlineSixCompressed
          override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueFullyCompressed.DeadlineSevenCompressed
          override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueFullyCompressed.DeadlineFullyCompressed
          override def deadlineUncompressed: Deadline.Uncompressed = ValueFullyCompressed.DeadlineUncompressed
        }
        object ValueFullyCompressed extends ValueFullyCompressed {
          object NoDeadline extends BaseEntryId(2410) with Deadline.NoDeadline with ValueFullyCompressed
          object DeadlineOneCompressed extends BaseEntryId(2411) with Deadline.OneCompressed with ValueFullyCompressed
          object DeadlineTwoCompressed extends BaseEntryId(2412) with Deadline.TwoCompressed with ValueFullyCompressed
          object DeadlineThreeCompressed extends BaseEntryId(2413) with Deadline.ThreeCompressed with ValueFullyCompressed
          object DeadlineFourCompressed extends BaseEntryId(2414) with Deadline.FourCompressed with ValueFullyCompressed
          object DeadlineFiveCompressed extends BaseEntryId(2415) with Deadline.FiveCompressed with ValueFullyCompressed
          object DeadlineSixCompressed extends BaseEntryId(2416) with Deadline.SixCompressed with ValueFullyCompressed
          object DeadlineSevenCompressed extends BaseEntryId(2417) with Deadline.SevenCompressed with ValueFullyCompressed
          object DeadlineFullyCompressed extends BaseEntryId(2418) with Deadline.FullyCompressed with ValueFullyCompressed
          object DeadlineUncompressed extends BaseEntryId(2419) with Deadline.Uncompressed with ValueFullyCompressed
        }
      }

      trait NoTime extends Time.NoTime with KeyUncompressed {
        override def noValue: Value.NoValue = NoTime.NoValue
        override def valueFullyCompressed: Value.FullyCompressed = NoTime.ValueFullyCompressed
        override def valueUncompressed: Value.Uncompressed = NoTime.ValueUncompressed
      }
      object NoTime extends NoTime {

        trait ValueUncompressed extends Value.Uncompressed with NoTime {
          override def valueOffsetOneCompressed: ValueOffset.OneCompressed = ValueUncompressed.ValueOffsetOneCompressed
          override def valueOffsetTwoCompressed: ValueOffset.TwoCompressed = ValueUncompressed.ValueOffsetTwoCompressed
          override def valueOffsetThreeCompressed: ValueOffset.ThreeCompressed = ValueUncompressed.ValueOffsetThreeCompressed
          override def valueOffsetUncompressed: ValueOffset.Uncompressed = ValueUncompressed.ValueOffsetUncompressed
        }
        object ValueUncompressed extends ValueUncompressed {

          trait ValueOffsetOneCompressed extends ValueOffset.OneCompressed with ValueUncompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetOneCompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetOneCompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetOneCompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetOneCompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetOneCompressed.ValueLengthUncompressed
          }
          object ValueOffsetOneCompressed extends ValueOffsetOneCompressed {
            trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetOneCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthOneCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthOneCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthOneCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthOneCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthOneCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthOneCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthOneCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthOneCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthOneCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthOneCompressed.DeadlineUncompressed
            }

            object ValueLengthOneCompressed extends ValueLengthOneCompressed {
              object NoDeadline extends BaseEntryId(2420) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryId(2421) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryId(2422) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryId(2423) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryId(2424) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryId(2425) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryId(2426) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryId(2427) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryId(2428) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryId(2429) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetOneCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthTwoCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthTwoCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthTwoCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthTwoCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthTwoCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthTwoCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthTwoCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthTwoCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthTwoCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthTwoCompressed.DeadlineUncompressed
            }

            object ValueLengthTwoCompressed extends ValueLengthTwoCompressed {
              object NoDeadline extends BaseEntryId(2430) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryId(2431) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryId(2432) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryId(2433) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryId(2434) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryId(2435) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryId(2436) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryId(2437) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryId(2438) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryId(2439) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetOneCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthThreeCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthThreeCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthThreeCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthThreeCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthThreeCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthThreeCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthThreeCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthThreeCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthThreeCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthThreeCompressed.DeadlineUncompressed
            }

            object ValueLengthThreeCompressed extends ValueLengthThreeCompressed {
              object NoDeadline extends BaseEntryId(2440) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryId(2441) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryId(2442) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryId(2443) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryId(2444) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryId(2445) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryId(2446) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryId(2447) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryId(2448) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryId(2449) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetOneCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthFullyCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthFullyCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthFullyCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthFullyCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthFullyCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthFullyCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthFullyCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthFullyCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthFullyCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthFullyCompressed.DeadlineUncompressed
            }

            object ValueLengthFullyCompressed extends ValueLengthFullyCompressed {
              object NoDeadline extends BaseEntryId(2450) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryId(2451) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryId(2452) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryId(2453) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryId(2454) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryId(2455) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryId(2456) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryId(2457) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryId(2458) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryId(2459) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetOneCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthUncompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthUncompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthUncompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthUncompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthUncompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthUncompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthUncompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthUncompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthUncompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthUncompressed.DeadlineUncompressed
            }

            object ValueLengthUncompressed extends ValueLengthUncompressed {
              object NoDeadline extends BaseEntryId(2460) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryId(2461) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryId(2462) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryId(2463) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryId(2464) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryId(2465) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryId(2466) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryId(2467) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryId(2468) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryId(2469) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }

          trait ValueOffsetTwoCompressed extends ValueOffset.TwoCompressed with ValueUncompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetTwoCompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetTwoCompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetTwoCompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetTwoCompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetTwoCompressed.ValueLengthUncompressed
          }
          object ValueOffsetTwoCompressed extends ValueOffsetTwoCompressed {
            trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetTwoCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthOneCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthOneCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthOneCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthOneCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthOneCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthOneCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthOneCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthOneCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthOneCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthOneCompressed.DeadlineUncompressed
            }

            object ValueLengthOneCompressed extends ValueLengthOneCompressed {
              object NoDeadline extends BaseEntryId(2470) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryId(2471) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryId(2472) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryId(2473) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryId(2474) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryId(2475) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryId(2476) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryId(2477) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryId(2478) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryId(2479) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetTwoCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthTwoCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthTwoCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthTwoCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthTwoCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthTwoCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthTwoCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthTwoCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthTwoCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthTwoCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthTwoCompressed.DeadlineUncompressed
            }

            object ValueLengthTwoCompressed extends ValueLengthTwoCompressed {
              object NoDeadline extends BaseEntryId(2480) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryId(2481) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryId(2482) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryId(2483) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryId(2484) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryId(2485) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryId(2486) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryId(2487) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryId(2488) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryId(2489) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetTwoCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthThreeCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthThreeCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthThreeCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthThreeCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthThreeCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthThreeCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthThreeCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthThreeCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthThreeCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthThreeCompressed.DeadlineUncompressed
            }

            object ValueLengthThreeCompressed extends ValueLengthThreeCompressed {
              object NoDeadline extends BaseEntryId(2490) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryId(2491) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryId(2492) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryId(2493) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryId(2494) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryId(2495) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryId(2496) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryId(2497) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryId(2498) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryId(2499) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetTwoCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthFullyCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthFullyCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthFullyCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthFullyCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthFullyCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthFullyCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthFullyCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthFullyCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthFullyCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthFullyCompressed.DeadlineUncompressed
            }

            object ValueLengthFullyCompressed extends ValueLengthFullyCompressed {
              object NoDeadline extends BaseEntryId(2500) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryId(2501) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryId(2502) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryId(2503) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryId(2504) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryId(2505) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryId(2506) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryId(2507) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryId(2508) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryId(2509) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetTwoCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthUncompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthUncompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthUncompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthUncompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthUncompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthUncompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthUncompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthUncompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthUncompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthUncompressed.DeadlineUncompressed
            }

            object ValueLengthUncompressed extends ValueLengthUncompressed {
              object NoDeadline extends BaseEntryId(2510) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryId(2511) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryId(2512) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryId(2513) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryId(2514) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryId(2515) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryId(2516) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryId(2517) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryId(2518) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryId(2519) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }

          trait ValueOffsetThreeCompressed extends ValueOffset.ThreeCompressed with ValueUncompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetThreeCompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetThreeCompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetThreeCompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetThreeCompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetThreeCompressed.ValueLengthUncompressed
          }
          object ValueOffsetThreeCompressed extends ValueOffsetThreeCompressed {
            trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetThreeCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthOneCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthOneCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthOneCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthOneCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthOneCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthOneCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthOneCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthOneCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthOneCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthOneCompressed.DeadlineUncompressed
            }

            object ValueLengthOneCompressed extends ValueLengthOneCompressed {
              object NoDeadline extends BaseEntryId(2520) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryId(2521) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryId(2522) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryId(2523) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryId(2524) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryId(2525) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryId(2526) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryId(2527) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryId(2528) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryId(2529) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetThreeCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthTwoCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthTwoCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthTwoCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthTwoCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthTwoCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthTwoCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthTwoCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthTwoCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthTwoCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthTwoCompressed.DeadlineUncompressed
            }

            object ValueLengthTwoCompressed extends ValueLengthTwoCompressed {
              object NoDeadline extends BaseEntryId(2530) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryId(2531) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryId(2532) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryId(2533) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryId(2534) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryId(2535) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryId(2536) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryId(2537) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryId(2538) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryId(2539) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetThreeCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthThreeCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthThreeCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthThreeCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthThreeCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthThreeCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthThreeCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthThreeCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthThreeCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthThreeCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthThreeCompressed.DeadlineUncompressed
            }

            object ValueLengthThreeCompressed extends ValueLengthThreeCompressed {
              object NoDeadline extends BaseEntryId(2540) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryId(2541) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryId(2542) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryId(2543) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryId(2544) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryId(2545) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryId(2546) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryId(2547) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryId(2548) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryId(2549) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetThreeCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthFullyCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthFullyCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthFullyCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthFullyCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthFullyCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthFullyCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthFullyCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthFullyCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthFullyCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthFullyCompressed.DeadlineUncompressed
            }

            object ValueLengthFullyCompressed extends ValueLengthFullyCompressed {
              object NoDeadline extends BaseEntryId(2550) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryId(2551) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryId(2552) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryId(2553) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryId(2554) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryId(2555) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryId(2556) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryId(2557) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryId(2558) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryId(2559) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetThreeCompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthUncompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthUncompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthUncompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthUncompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthUncompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthUncompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthUncompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthUncompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthUncompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthUncompressed.DeadlineUncompressed
            }

            object ValueLengthUncompressed extends ValueLengthUncompressed {
              object NoDeadline extends BaseEntryId(2560) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryId(2561) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryId(2562) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryId(2563) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryId(2564) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryId(2565) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryId(2566) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryId(2567) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryId(2568) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryId(2569) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }

          trait ValueOffsetUncompressed extends ValueOffset.Uncompressed with ValueUncompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetUncompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetUncompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetUncompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetUncompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetUncompressed.ValueLengthUncompressed
          }
          object ValueOffsetUncompressed extends ValueOffsetUncompressed {
            trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetUncompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthOneCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthOneCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthOneCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthOneCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthOneCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthOneCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthOneCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthOneCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthOneCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthOneCompressed.DeadlineUncompressed
            }

            object ValueLengthOneCompressed extends ValueLengthOneCompressed {
              object NoDeadline extends BaseEntryId(2570) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryId(2571) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryId(2572) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryId(2573) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryId(2574) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryId(2575) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryId(2576) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryId(2577) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryId(2578) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryId(2579) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetUncompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthTwoCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthTwoCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthTwoCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthTwoCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthTwoCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthTwoCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthTwoCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthTwoCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthTwoCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthTwoCompressed.DeadlineUncompressed
            }

            object ValueLengthTwoCompressed extends ValueLengthTwoCompressed {
              object NoDeadline extends BaseEntryId(2580) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryId(2581) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryId(2582) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryId(2583) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryId(2584) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryId(2585) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryId(2586) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryId(2587) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryId(2588) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryId(2589) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetUncompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthThreeCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthThreeCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthThreeCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthThreeCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthThreeCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthThreeCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthThreeCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthThreeCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthThreeCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthThreeCompressed.DeadlineUncompressed
            }

            object ValueLengthThreeCompressed extends ValueLengthThreeCompressed {
              object NoDeadline extends BaseEntryId(2590) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryId(2591) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryId(2592) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryId(2593) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryId(2594) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryId(2595) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryId(2596) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryId(2597) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryId(2598) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryId(2599) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetUncompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthFullyCompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthFullyCompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthFullyCompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthFullyCompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthFullyCompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthFullyCompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthFullyCompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthFullyCompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthFullyCompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthFullyCompressed.DeadlineUncompressed
            }

            object ValueLengthFullyCompressed extends ValueLengthFullyCompressed {
              object NoDeadline extends BaseEntryId(2600) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryId(2601) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryId(2602) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryId(2603) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryId(2604) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryId(2605) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryId(2606) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryId(2607) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryId(2608) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryId(2609) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetUncompressed {
              override def noDeadline: Deadline.NoDeadline = ValueLengthUncompressed.NoDeadline
              override def deadlineOneCompressed: Deadline.OneCompressed = ValueLengthUncompressed.DeadlineOneCompressed
              override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueLengthUncompressed.DeadlineTwoCompressed
              override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueLengthUncompressed.DeadlineThreeCompressed
              override def deadlineFourCompressed: Deadline.FourCompressed = ValueLengthUncompressed.DeadlineFourCompressed
              override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueLengthUncompressed.DeadlineFiveCompressed
              override def deadlineSixCompressed: Deadline.SixCompressed = ValueLengthUncompressed.DeadlineSixCompressed
              override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueLengthUncompressed.DeadlineSevenCompressed
              override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueLengthUncompressed.DeadlineFullyCompressed
              override def deadlineUncompressed: Deadline.Uncompressed = ValueLengthUncompressed.DeadlineUncompressed
            }

            object ValueLengthUncompressed extends ValueLengthUncompressed {
              object NoDeadline extends BaseEntryId(2610) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryId(2611) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryId(2612) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryId(2613) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryId(2614) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryId(2615) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryId(2616) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryId(2617) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryId(2618) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryId(2619) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }
        }

        trait NoValue extends Value.NoValue with NoTime {
          override def noDeadline: Deadline.NoDeadline = NoValue.NoDeadline
          override def deadlineOneCompressed: Deadline.OneCompressed = NoValue.DeadlineOneCompressed
          override def deadlineTwoCompressed: Deadline.TwoCompressed = NoValue.DeadlineTwoCompressed
          override def deadlineThreeCompressed: Deadline.ThreeCompressed = NoValue.DeadlineThreeCompressed
          override def deadlineFourCompressed: Deadline.FourCompressed = NoValue.DeadlineFourCompressed
          override def deadlineFiveCompressed: Deadline.FiveCompressed = NoValue.DeadlineFiveCompressed
          override def deadlineSixCompressed: Deadline.SixCompressed = NoValue.DeadlineSixCompressed
          override def deadlineSevenCompressed: Deadline.SevenCompressed = NoValue.DeadlineSevenCompressed
          override def deadlineFullyCompressed: Deadline.FullyCompressed = NoValue.DeadlineFullyCompressed
          override def deadlineUncompressed: Deadline.Uncompressed = NoValue.DeadlineUncompressed
        }
        object NoValue extends NoValue {
          object NoDeadline extends BaseEntryId(2620) with Deadline.NoDeadline with NoValue
          object DeadlineOneCompressed extends BaseEntryId(2621) with Deadline.OneCompressed with NoValue
          object DeadlineTwoCompressed extends BaseEntryId(2622) with Deadline.TwoCompressed with NoValue
          object DeadlineThreeCompressed extends BaseEntryId(2623) with Deadline.ThreeCompressed with NoValue
          object DeadlineFourCompressed extends BaseEntryId(2624) with Deadline.FourCompressed with NoValue
          object DeadlineFiveCompressed extends BaseEntryId(2625) with Deadline.FiveCompressed with NoValue
          object DeadlineSixCompressed extends BaseEntryId(2626) with Deadline.SixCompressed with NoValue
          object DeadlineSevenCompressed extends BaseEntryId(2627) with Deadline.SevenCompressed with NoValue
          object DeadlineFullyCompressed extends BaseEntryId(2628) with Deadline.FullyCompressed with NoValue
          object DeadlineUncompressed extends BaseEntryId(2629) with Deadline.Uncompressed with NoValue
        }

        trait ValueFullyCompressed extends Value.FullyCompressed with NoTime {
          override def noDeadline: Deadline.NoDeadline = ValueFullyCompressed.NoDeadline
          override def deadlineOneCompressed: Deadline.OneCompressed = ValueFullyCompressed.DeadlineOneCompressed
          override def deadlineTwoCompressed: Deadline.TwoCompressed = ValueFullyCompressed.DeadlineTwoCompressed
          override def deadlineThreeCompressed: Deadline.ThreeCompressed = ValueFullyCompressed.DeadlineThreeCompressed
          override def deadlineFourCompressed: Deadline.FourCompressed = ValueFullyCompressed.DeadlineFourCompressed
          override def deadlineFiveCompressed: Deadline.FiveCompressed = ValueFullyCompressed.DeadlineFiveCompressed
          override def deadlineSixCompressed: Deadline.SixCompressed = ValueFullyCompressed.DeadlineSixCompressed
          override def deadlineSevenCompressed: Deadline.SevenCompressed = ValueFullyCompressed.DeadlineSevenCompressed
          override def deadlineFullyCompressed: Deadline.FullyCompressed = ValueFullyCompressed.DeadlineFullyCompressed
          override def deadlineUncompressed: Deadline.Uncompressed = ValueFullyCompressed.DeadlineUncompressed
        }
        object ValueFullyCompressed extends ValueFullyCompressed {
          object NoDeadline extends BaseEntryId(2630) with Deadline.NoDeadline with ValueFullyCompressed
          object DeadlineOneCompressed extends BaseEntryId(2631) with Deadline.OneCompressed with ValueFullyCompressed
          object DeadlineTwoCompressed extends BaseEntryId(2632) with Deadline.TwoCompressed with ValueFullyCompressed
          object DeadlineThreeCompressed extends BaseEntryId(2633) with Deadline.ThreeCompressed with ValueFullyCompressed
          object DeadlineFourCompressed extends BaseEntryId(2634) with Deadline.FourCompressed with ValueFullyCompressed
          object DeadlineFiveCompressed extends BaseEntryId(2635) with Deadline.FiveCompressed with ValueFullyCompressed
          object DeadlineSixCompressed extends BaseEntryId(2636) with Deadline.SixCompressed with ValueFullyCompressed
          object DeadlineSevenCompressed extends BaseEntryId(2637) with Deadline.SevenCompressed with ValueFullyCompressed
          object DeadlineFullyCompressed extends BaseEntryId(2638) with Deadline.FullyCompressed with ValueFullyCompressed
          object DeadlineUncompressed extends BaseEntryId(2639) with Deadline.Uncompressed with ValueFullyCompressed
        }
      }
    }
  }

  override def keyIdsList: List[BaseEntryId] = SealedList.list[BaseEntryId].sortBy(_.id)
}