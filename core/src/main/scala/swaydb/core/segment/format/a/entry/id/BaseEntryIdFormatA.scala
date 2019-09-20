
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.segment.format.a.entry.id

import swaydb.core.segment.format.a.entry.id.BaseEntryId._
import swaydb.macros.SealedList

private[core] sealed abstract class BaseEntryIdFormatA(override val baseId: Int) extends BaseEntryId(baseId)
private[core] object BaseEntryIdFormatA extends BaseEntryIdFormat {

  override def format: BaseEntryId.Format = BaseEntryIdFormatA.FormatA1

  sealed trait FormatA1 extends Format.A {
    override def start: Key = FormatA1.KeyStart
  }
  object FormatA1 extends FormatA1 {

    sealed trait KeyStart extends Key with FormatA1 {
      override def noTime: Time.NoTime = KeyStart.NoTime
      override def timePartiallyCompressed: Time.PartiallyCompressed = KeyStart.TimePartiallyCompressed
      override def timeUncompressed: Time.Uncompressed = KeyStart.TimeUncompressed
    }
    object KeyStart extends KeyStart {

      sealed trait TimePartiallyCompressed extends Time.PartiallyCompressed with KeyStart {
        override def noValue: Value.NoValue = TimePartiallyCompressed.NoValue
        override def valueFullyCompressed: Value.FullyCompressed = TimePartiallyCompressed.ValueFullyCompressed
        override def valueUncompressed: Value.Uncompressed = TimePartiallyCompressed.ValueUncompressed
      }
      object TimePartiallyCompressed extends TimePartiallyCompressed {

        sealed trait ValueUncompressed extends Value.Uncompressed with TimePartiallyCompressed {
          override def valueOffsetOneCompressed: ValueOffset.OneCompressed = ValueUncompressed.ValueOffsetOneCompressed
          override def valueOffsetTwoCompressed: ValueOffset.TwoCompressed = ValueUncompressed.ValueOffsetTwoCompressed
          override def valueOffsetThreeCompressed: ValueOffset.ThreeCompressed = ValueUncompressed.ValueOffsetThreeCompressed
          override def valueOffsetUncompressed: ValueOffset.Uncompressed = ValueUncompressed.ValueOffsetUncompressed
        }
        object ValueUncompressed extends ValueUncompressed {

          sealed trait ValueOffsetOneCompressed extends ValueOffset.OneCompressed with ValueUncompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetOneCompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetOneCompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetOneCompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetOneCompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetOneCompressed.ValueLengthUncompressed
          }
          object ValueOffsetOneCompressed extends ValueOffsetOneCompressed {
            sealed trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetOneCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(0) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(138) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(139) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(140) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(141) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(142) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(143) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(144) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(145) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(146) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            sealed trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetOneCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(1) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(147) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(148) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(149) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(150) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(151) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(152) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(153) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(154) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(155) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            sealed trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetOneCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(2) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(156) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(157) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(158) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(159) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(160) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(161) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(162) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(163) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(164) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            sealed trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetOneCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(3) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(165) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(166) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(167) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(168) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(169) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(170) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(171) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(172) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(173) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            sealed trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetOneCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(4) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(174) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(175) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(176) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(177) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(178) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(179) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(180) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(181) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(182) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }

          sealed trait ValueOffsetTwoCompressed extends ValueOffset.TwoCompressed with ValueUncompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetTwoCompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetTwoCompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetTwoCompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetTwoCompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetTwoCompressed.ValueLengthUncompressed
          }
          object ValueOffsetTwoCompressed extends ValueOffsetTwoCompressed {
            sealed trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetTwoCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(5) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(183) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(184) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(185) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(186) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(187) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(188) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(189) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(190) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(191) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            sealed trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetTwoCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(6) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(192) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(193) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(194) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(195) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(196) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(197) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(198) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(199) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(200) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            sealed trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetTwoCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(7) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(201) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(202) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(203) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(204) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(205) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(206) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(207) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(208) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(209) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            sealed trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetTwoCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(8) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(210) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(211) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(212) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(213) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(214) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(215) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(216) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(217) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(218) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            sealed trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetTwoCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(9) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(219) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(220) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(221) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(222) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(223) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(224) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(225) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(226) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(227) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }

          sealed trait ValueOffsetThreeCompressed extends ValueOffset.ThreeCompressed with ValueUncompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetThreeCompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetThreeCompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetThreeCompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetThreeCompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetThreeCompressed.ValueLengthUncompressed
          }
          object ValueOffsetThreeCompressed extends ValueOffsetThreeCompressed {
            sealed trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetThreeCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(10) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(228) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(229) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(230) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(231) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(232) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(233) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(234) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(235) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(236) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            sealed trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetThreeCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(11) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(237) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(238) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(239) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(240) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(241) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(242) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(243) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(244) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(245) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            sealed trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetThreeCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(12) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(246) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(247) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(248) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(249) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(250) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(251) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(252) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(253) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(254) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            sealed trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetThreeCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(13) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(255) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(256) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(257) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(258) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(259) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(260) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(261) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(262) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(263) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            sealed trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetThreeCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(14) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(264) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(265) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(266) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(267) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(268) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(269) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(270) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(271) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(272) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }

          sealed trait ValueOffsetUncompressed extends ValueOffset.Uncompressed with ValueUncompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetUncompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetUncompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetUncompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetUncompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetUncompressed.ValueLengthUncompressed
          }
          object ValueOffsetUncompressed extends ValueOffsetUncompressed {
            sealed trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetUncompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(15) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(273) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(274) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(275) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(276) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(277) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(278) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(279) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(280) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(281) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            sealed trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetUncompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(16) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(282) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(283) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(284) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(285) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(286) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(287) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(288) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(289) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(290) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            sealed trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetUncompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(17) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(291) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(292) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(293) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(294) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(295) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(296) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(297) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(298) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(299) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            sealed trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetUncompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(18) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(300) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(301) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(302) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(303) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(304) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(305) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(306) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(307) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(308) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            sealed trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetUncompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(19) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(309) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(310) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(311) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(312) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(313) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(314) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(315) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(316) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(317) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }
        }

        sealed trait NoValue extends Value.NoValue with TimePartiallyCompressed {
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
          object NoDeadline extends BaseEntryIdFormatA(20) with Deadline.NoDeadline with NoValue
          object DeadlineOneCompressed extends BaseEntryIdFormatA(318) with Deadline.OneCompressed with NoValue
          object DeadlineTwoCompressed extends BaseEntryIdFormatA(319) with Deadline.TwoCompressed with NoValue
          object DeadlineThreeCompressed extends BaseEntryIdFormatA(320) with Deadline.ThreeCompressed with NoValue
          object DeadlineFourCompressed extends BaseEntryIdFormatA(321) with Deadline.FourCompressed with NoValue
          object DeadlineFiveCompressed extends BaseEntryIdFormatA(322) with Deadline.FiveCompressed with NoValue
          object DeadlineSixCompressed extends BaseEntryIdFormatA(323) with Deadline.SixCompressed with NoValue
          object DeadlineSevenCompressed extends BaseEntryIdFormatA(324) with Deadline.SevenCompressed with NoValue
          object DeadlineFullyCompressed extends BaseEntryIdFormatA(325) with Deadline.FullyCompressed with NoValue
          object DeadlineUncompressed extends BaseEntryIdFormatA(326) with Deadline.Uncompressed with NoValue
        }

        sealed trait ValueFullyCompressed extends Value.FullyCompressed with TimePartiallyCompressed {
          override def valueOffsetOneCompressed: ValueOffset.OneCompressed = ValueFullyCompressed.ValueOffsetOneCompressed
          override def valueOffsetTwoCompressed: ValueOffset.TwoCompressed = ValueFullyCompressed.ValueOffsetTwoCompressed
          override def valueOffsetThreeCompressed: ValueOffset.ThreeCompressed = ValueFullyCompressed.ValueOffsetThreeCompressed
          override def valueOffsetUncompressed: ValueOffset.Uncompressed = ValueFullyCompressed.ValueOffsetUncompressed
          override def valueOffsetFullyCompressed: ValueOffset.FullyCompressed = ValueFullyCompressed.ValueOffsetFullyCompressed
        }
        object ValueFullyCompressed extends ValueFullyCompressed {
          sealed trait ValueOffsetOneCompressed extends ValueOffset.OneCompressed with ValueFullyCompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetOneCompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetOneCompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetOneCompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetOneCompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetOneCompressed.ValueLengthUncompressed
          }
          object ValueOffsetOneCompressed extends ValueOffsetOneCompressed {
            sealed trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetOneCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(21) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(327) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(328) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(329) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(330) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(331) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(332) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(333) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(334) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(335) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            sealed trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetOneCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(22) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(336) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(337) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(338) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(339) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(340) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(341) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(342) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(343) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(344) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            sealed trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetOneCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(23) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(345) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(346) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(347) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(348) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(349) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(350) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(351) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(352) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(353) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            sealed trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetOneCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(24) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(354) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(355) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(356) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(357) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(358) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(359) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(360) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(361) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(362) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            sealed trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetOneCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(25) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(363) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(364) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(365) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(366) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(367) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(368) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(369) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(370) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(371) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }

          sealed trait ValueOffsetTwoCompressed extends ValueOffset.TwoCompressed with ValueFullyCompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetTwoCompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetTwoCompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetTwoCompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetTwoCompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetTwoCompressed.ValueLengthUncompressed
          }
          object ValueOffsetTwoCompressed extends ValueOffsetTwoCompressed {
            sealed trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetTwoCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(26) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(372) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(373) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(374) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(375) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(376) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(377) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(378) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(379) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(380) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            sealed trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetTwoCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(27) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(381) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(382) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(383) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(384) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(385) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(386) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(387) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(388) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(389) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            sealed trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetTwoCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(28) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(390) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(391) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(392) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(393) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(394) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(395) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(396) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(397) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(398) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            sealed trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetTwoCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(29) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(399) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(400) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(401) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(402) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(403) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(404) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(405) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(406) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(407) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            sealed trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetTwoCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(30) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(408) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(409) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(410) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(411) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(412) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(413) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(414) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(415) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(416) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }

          sealed trait ValueOffsetThreeCompressed extends ValueOffset.ThreeCompressed with ValueFullyCompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetThreeCompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetThreeCompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetThreeCompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetThreeCompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetThreeCompressed.ValueLengthUncompressed
          }
          object ValueOffsetThreeCompressed extends ValueOffsetThreeCompressed {
            sealed trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetThreeCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(31) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(417) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(418) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(419) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(420) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(421) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(422) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(423) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(424) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(425) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            sealed trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetThreeCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(32) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(426) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(427) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(428) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(429) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(430) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(431) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(432) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(433) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(434) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            sealed trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetThreeCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(33) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(435) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(436) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(437) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(438) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(439) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(440) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(441) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(442) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(443) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            sealed trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetThreeCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(34) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(444) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(445) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(446) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(447) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(448) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(449) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(450) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(451) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(452) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            sealed trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetThreeCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(35) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(453) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(454) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(455) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(456) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(457) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(458) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(459) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(460) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(461) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }

          sealed trait ValueOffsetFullyCompressed extends ValueOffset.FullyCompressed with ValueFullyCompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetFullyCompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetFullyCompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetFullyCompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetFullyCompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetFullyCompressed.ValueLengthUncompressed
          }
          object ValueOffsetFullyCompressed extends ValueOffsetFullyCompressed {
            sealed trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetFullyCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(36) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(462) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(463) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(464) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(465) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(466) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(467) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(468) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(469) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(470) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            sealed trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetFullyCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(37) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(471) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(472) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(473) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(474) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(475) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(476) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(477) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(478) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(479) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            sealed trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetFullyCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(38) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(480) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(481) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(482) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(483) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(484) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(485) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(486) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(487) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(488) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            sealed trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetFullyCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(39) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(489) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(490) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(491) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(492) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(493) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(494) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(495) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(496) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(497) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            sealed trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetFullyCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(40) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(498) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(499) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(500) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(501) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(502) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(503) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(504) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(505) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(506) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }

          sealed trait ValueOffsetUncompressed extends ValueOffset.Uncompressed with ValueFullyCompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetUncompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetUncompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetUncompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetUncompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetUncompressed.ValueLengthUncompressed
          }
          object ValueOffsetUncompressed extends ValueOffsetUncompressed {
            sealed trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetUncompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(41) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(507) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(508) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(509) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(510) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(511) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(512) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(513) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(514) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(515) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            sealed trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetUncompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(42) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(516) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(517) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(518) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(519) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(520) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(521) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(522) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(523) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(524) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            sealed trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetUncompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(43) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(525) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(526) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(527) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(528) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(529) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(530) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(531) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(532) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(533) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            sealed trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetUncompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(44) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(534) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(535) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(536) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(537) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(538) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(539) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(540) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(541) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(542) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            sealed trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetUncompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(45) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(543) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(544) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(545) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(546) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(547) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(548) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(549) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(550) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(551) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }
        }
      }

      sealed trait NoTime extends Time.NoTime with KeyStart {
        override def noValue: Value.NoValue = NoTime.NoValue
        override def valueFullyCompressed: Value.FullyCompressed = NoTime.ValueFullyCompressed
        override def valueUncompressed: Value.Uncompressed = NoTime.ValueUncompressed
      }
      object NoTime extends NoTime {

        sealed trait ValueUncompressed extends Value.Uncompressed with NoTime {
          override def valueOffsetOneCompressed: ValueOffset.OneCompressed = ValueUncompressed.ValueOffsetOneCompressed
          override def valueOffsetTwoCompressed: ValueOffset.TwoCompressed = ValueUncompressed.ValueOffsetTwoCompressed
          override def valueOffsetThreeCompressed: ValueOffset.ThreeCompressed = ValueUncompressed.ValueOffsetThreeCompressed
          override def valueOffsetUncompressed: ValueOffset.Uncompressed = ValueUncompressed.ValueOffsetUncompressed
        }
        object ValueUncompressed extends ValueUncompressed {

          sealed trait ValueOffsetOneCompressed extends ValueOffset.OneCompressed with ValueUncompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetOneCompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetOneCompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetOneCompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetOneCompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetOneCompressed.ValueLengthUncompressed
          }
          object ValueOffsetOneCompressed extends ValueOffsetOneCompressed {
            sealed trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetOneCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(46) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(552) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(553) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(554) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(555) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(556) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(557) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(558) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(559) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(560) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            sealed trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetOneCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(47) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(561) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(562) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(563) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(564) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(565) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(566) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(567) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(568) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(569) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            sealed trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetOneCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(48) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(570) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(571) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(572) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(573) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(574) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(575) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(576) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(577) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(578) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            sealed trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetOneCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(49) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(579) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(580) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(581) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(582) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(583) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(584) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(585) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(586) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(587) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            sealed trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetOneCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(50) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(588) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(589) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(590) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(591) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(592) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(593) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(594) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(595) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(596) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }

          sealed trait ValueOffsetTwoCompressed extends ValueOffset.TwoCompressed with ValueUncompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetTwoCompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetTwoCompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetTwoCompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetTwoCompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetTwoCompressed.ValueLengthUncompressed
          }
          object ValueOffsetTwoCompressed extends ValueOffsetTwoCompressed {
            sealed trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetTwoCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(51) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(597) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(598) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(599) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(600) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(601) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(602) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(603) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(604) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(605) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            sealed trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetTwoCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(52) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(606) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(607) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(608) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(609) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(610) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(611) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(612) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(613) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(614) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            sealed trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetTwoCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(53) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(615) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(616) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(617) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(618) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(619) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(620) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(621) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(622) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(623) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            sealed trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetTwoCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(54) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(624) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(625) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(626) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(627) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(628) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(629) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(630) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(631) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(632) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            sealed trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetTwoCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(55) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(633) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(634) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(635) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(636) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(637) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(638) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(639) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(640) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(641) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }

          sealed trait ValueOffsetThreeCompressed extends ValueOffset.ThreeCompressed with ValueUncompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetThreeCompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetThreeCompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetThreeCompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetThreeCompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetThreeCompressed.ValueLengthUncompressed
          }
          object ValueOffsetThreeCompressed extends ValueOffsetThreeCompressed {
            sealed trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetThreeCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(56) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(642) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(643) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(644) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(645) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(646) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(647) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(648) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(649) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(650) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            sealed trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetThreeCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(57) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(651) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(652) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(653) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(654) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(655) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(656) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(657) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(658) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(659) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            sealed trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetThreeCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(58) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(660) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(661) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(662) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(663) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(664) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(665) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(666) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(667) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(668) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            sealed trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetThreeCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(59) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(669) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(670) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(671) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(672) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(673) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(674) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(675) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(676) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(677) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            sealed trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetThreeCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(60) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(678) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(679) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(680) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(681) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(682) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(683) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(684) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(685) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(686) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }

          sealed trait ValueOffsetUncompressed extends ValueOffset.Uncompressed with ValueUncompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetUncompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetUncompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetUncompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetUncompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetUncompressed.ValueLengthUncompressed
          }
          object ValueOffsetUncompressed extends ValueOffsetUncompressed {
            sealed trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetUncompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(61) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(687) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(688) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(689) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(690) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(691) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(692) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(693) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(694) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(695) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            sealed trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetUncompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(62) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(696) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(697) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(698) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(699) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(700) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(701) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(702) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(703) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(704) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            sealed trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetUncompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(63) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(705) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(706) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(707) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(708) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(709) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(710) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(711) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(712) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(713) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            sealed trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetUncompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(64) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(714) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(715) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(716) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(717) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(718) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(719) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(720) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(721) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(722) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            sealed trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetUncompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(65) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(723) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(724) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(725) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(726) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(727) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(728) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(729) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(730) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(731) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }
        }

        sealed trait NoValue extends Value.NoValue with NoTime {
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
          object NoDeadline extends BaseEntryIdFormatA(66) with Deadline.NoDeadline with NoValue
          object DeadlineOneCompressed extends BaseEntryIdFormatA(732) with Deadline.OneCompressed with NoValue
          object DeadlineTwoCompressed extends BaseEntryIdFormatA(733) with Deadline.TwoCompressed with NoValue
          object DeadlineThreeCompressed extends BaseEntryIdFormatA(734) with Deadline.ThreeCompressed with NoValue
          object DeadlineFourCompressed extends BaseEntryIdFormatA(735) with Deadline.FourCompressed with NoValue
          object DeadlineFiveCompressed extends BaseEntryIdFormatA(736) with Deadline.FiveCompressed with NoValue
          object DeadlineSixCompressed extends BaseEntryIdFormatA(737) with Deadline.SixCompressed with NoValue
          object DeadlineSevenCompressed extends BaseEntryIdFormatA(738) with Deadline.SevenCompressed with NoValue
          object DeadlineFullyCompressed extends BaseEntryIdFormatA(739) with Deadline.FullyCompressed with NoValue
          object DeadlineUncompressed extends BaseEntryIdFormatA(740) with Deadline.Uncompressed with NoValue
        }

        sealed trait ValueFullyCompressed extends Value.FullyCompressed with NoTime {
          override def valueOffsetOneCompressed: ValueOffset.OneCompressed = ValueFullyCompressed.ValueOffsetOneCompressed
          override def valueOffsetTwoCompressed: ValueOffset.TwoCompressed = ValueFullyCompressed.ValueOffsetTwoCompressed
          override def valueOffsetThreeCompressed: ValueOffset.ThreeCompressed = ValueFullyCompressed.ValueOffsetThreeCompressed
          override def valueOffsetUncompressed: ValueOffset.Uncompressed = ValueFullyCompressed.ValueOffsetUncompressed
          override def valueOffsetFullyCompressed: ValueOffset.FullyCompressed = ValueFullyCompressed.ValueOffsetFullyCompressed
        }
        object ValueFullyCompressed extends ValueFullyCompressed {
          sealed trait ValueOffsetOneCompressed extends ValueOffset.OneCompressed with ValueFullyCompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetOneCompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetOneCompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetOneCompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetOneCompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetOneCompressed.ValueLengthUncompressed
          }
          object ValueOffsetOneCompressed extends ValueOffsetOneCompressed {
            sealed trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetOneCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(67) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(741) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(742) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(743) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(744) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(745) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(746) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(747) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(748) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(749) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            sealed trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetOneCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(68) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(750) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(751) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(752) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(753) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(754) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(755) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(756) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(757) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(758) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            sealed trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetOneCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(69) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(759) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(760) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(761) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(762) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(763) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(764) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(765) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(766) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(767) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            sealed trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetOneCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(70) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(768) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(769) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(770) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(771) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(772) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(773) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(774) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(775) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(776) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            sealed trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetOneCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(71) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(777) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(778) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(779) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(780) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(781) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(782) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(783) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(784) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(785) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }

          sealed trait ValueOffsetTwoCompressed extends ValueOffset.TwoCompressed with ValueFullyCompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetTwoCompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetTwoCompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetTwoCompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetTwoCompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetTwoCompressed.ValueLengthUncompressed
          }
          object ValueOffsetTwoCompressed extends ValueOffsetTwoCompressed {
            sealed trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetTwoCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(72) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(786) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(787) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(788) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(789) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(790) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(791) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(792) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(793) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(794) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            sealed trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetTwoCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(73) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(795) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(796) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(797) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(798) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(799) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(800) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(801) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(802) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(803) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            sealed trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetTwoCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(74) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(804) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(805) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(806) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(807) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(808) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(809) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(810) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(811) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(812) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            sealed trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetTwoCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(75) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(813) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(814) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(815) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(816) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(817) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(818) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(819) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(820) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(821) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            sealed trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetTwoCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(76) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(822) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(823) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(824) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(825) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(826) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(827) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(828) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(829) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(830) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }

          sealed trait ValueOffsetThreeCompressed extends ValueOffset.ThreeCompressed with ValueFullyCompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetThreeCompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetThreeCompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetThreeCompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetThreeCompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetThreeCompressed.ValueLengthUncompressed
          }
          object ValueOffsetThreeCompressed extends ValueOffsetThreeCompressed {
            sealed trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetThreeCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(77) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(831) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(832) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(833) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(834) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(835) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(836) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(837) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(838) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(839) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            sealed trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetThreeCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(78) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(840) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(841) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(842) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(843) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(844) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(845) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(846) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(847) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(848) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            sealed trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetThreeCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(79) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(849) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(850) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(851) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(852) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(853) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(854) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(855) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(856) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(857) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            sealed trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetThreeCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(80) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(858) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(859) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(860) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(861) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(862) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(863) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(864) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(865) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(866) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            sealed trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetThreeCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(81) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(867) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(868) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(869) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(870) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(871) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(872) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(873) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(874) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(875) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }

          sealed trait ValueOffsetFullyCompressed extends ValueOffset.FullyCompressed with ValueFullyCompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetFullyCompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetFullyCompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetFullyCompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetFullyCompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetFullyCompressed.ValueLengthUncompressed
          }
          object ValueOffsetFullyCompressed extends ValueOffsetFullyCompressed {
            sealed trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetFullyCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(82) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(876) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(877) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(878) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(879) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(880) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(881) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(882) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(883) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(884) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            sealed trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetFullyCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(83) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(885) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(886) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(887) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(888) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(889) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(890) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(891) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(892) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(893) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            sealed trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetFullyCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(84) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(894) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(895) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(896) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(897) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(898) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(899) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(900) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(901) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(902) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            sealed trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetFullyCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(85) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(903) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(904) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(905) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(906) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(907) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(908) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(909) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(910) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(911) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            sealed trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetFullyCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(86) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(912) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(913) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(914) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(915) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(916) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(917) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(918) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(919) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(920) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }

          sealed trait ValueOffsetUncompressed extends ValueOffset.Uncompressed with ValueFullyCompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetUncompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetUncompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetUncompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetUncompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetUncompressed.ValueLengthUncompressed
          }
          object ValueOffsetUncompressed extends ValueOffsetUncompressed {
            sealed trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetUncompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(87) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(921) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(922) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(923) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(924) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(925) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(926) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(927) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(928) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(929) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            sealed trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetUncompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(88) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(930) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(931) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(932) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(933) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(934) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(935) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(936) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(937) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(938) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            sealed trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetUncompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(89) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(939) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(940) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(941) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(942) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(943) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(944) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(945) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(946) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(947) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            sealed trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetUncompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(90) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(948) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(949) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(950) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(951) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(952) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(953) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(954) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(955) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(956) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            sealed trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetUncompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(91) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(957) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(958) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(959) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(960) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(961) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(962) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(963) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(964) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(965) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }
        }
      }

      sealed trait TimeUncompressed extends Time.Uncompressed with KeyStart {
        override def noValue: Value.NoValue = TimeUncompressed.NoValue
        override def valueFullyCompressed: Value.FullyCompressed = TimeUncompressed.ValueFullyCompressed
        override def valueUncompressed: Value.Uncompressed = TimeUncompressed.ValueUncompressed
      }
      object TimeUncompressed extends TimeUncompressed {

        sealed trait ValueUncompressed extends Value.Uncompressed with TimeUncompressed {
          override def valueOffsetOneCompressed: ValueOffset.OneCompressed = ValueUncompressed.ValueOffsetOneCompressed
          override def valueOffsetTwoCompressed: ValueOffset.TwoCompressed = ValueUncompressed.ValueOffsetTwoCompressed
          override def valueOffsetThreeCompressed: ValueOffset.ThreeCompressed = ValueUncompressed.ValueOffsetThreeCompressed
          override def valueOffsetUncompressed: ValueOffset.Uncompressed = ValueUncompressed.ValueOffsetUncompressed
        }
        object ValueUncompressed extends ValueUncompressed {

          sealed trait ValueOffsetOneCompressed extends ValueOffset.OneCompressed with ValueUncompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetOneCompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetOneCompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetOneCompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetOneCompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetOneCompressed.ValueLengthUncompressed
          }
          object ValueOffsetOneCompressed extends ValueOffsetOneCompressed {
            sealed trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetOneCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(92) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(966) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(967) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(968) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(969) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(970) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(971) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(972) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(973) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(974) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            sealed trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetOneCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(93) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(975) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(976) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(977) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(978) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(979) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(980) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(981) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(982) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(983) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            sealed trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetOneCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(94) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(984) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(985) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(986) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(987) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(988) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(989) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(990) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(991) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(992) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            sealed trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetOneCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(95) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(993) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(994) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(995) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(996) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(997) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(998) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(999) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1000) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(1001) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            sealed trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetOneCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(96) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1002) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1003) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1004) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1005) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1006) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1007) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1008) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1009) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(1010) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }

          sealed trait ValueOffsetTwoCompressed extends ValueOffset.TwoCompressed with ValueUncompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetTwoCompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetTwoCompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetTwoCompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetTwoCompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetTwoCompressed.ValueLengthUncompressed
          }
          object ValueOffsetTwoCompressed extends ValueOffsetTwoCompressed {
            sealed trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetTwoCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(97) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1011) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1012) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1013) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1014) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1015) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1016) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1017) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1018) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(1019) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            sealed trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetTwoCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(98) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1020) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1021) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1022) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1023) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1024) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1025) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1026) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1027) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(1028) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            sealed trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetTwoCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(99) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1029) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1030) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1031) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1032) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1033) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1034) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1035) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1036) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(1037) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            sealed trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetTwoCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(100) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1038) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1039) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1040) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1041) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1042) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1043) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1044) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1045) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(1046) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            sealed trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetTwoCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(101) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1047) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1048) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1049) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1050) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1051) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1052) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1053) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1054) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(1055) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }

          sealed trait ValueOffsetThreeCompressed extends ValueOffset.ThreeCompressed with ValueUncompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetThreeCompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetThreeCompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetThreeCompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetThreeCompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetThreeCompressed.ValueLengthUncompressed
          }
          object ValueOffsetThreeCompressed extends ValueOffsetThreeCompressed {
            sealed trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetThreeCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(102) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1056) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1057) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1058) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1059) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1060) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1061) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1062) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1063) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(1064) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            sealed trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetThreeCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(103) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1065) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1066) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1067) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1068) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1069) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1070) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1071) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1072) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(1073) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            sealed trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetThreeCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(104) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1074) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1075) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1076) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1077) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1078) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1079) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1080) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1081) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(1082) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            sealed trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetThreeCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(105) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1083) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1084) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1085) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1086) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1087) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1088) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1089) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1090) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(1091) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            sealed trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetThreeCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(106) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1092) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1093) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1094) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1095) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1096) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1097) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1098) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1099) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(1100) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }

          sealed trait ValueOffsetUncompressed extends ValueOffset.Uncompressed with ValueUncompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetUncompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetUncompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetUncompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetUncompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetUncompressed.ValueLengthUncompressed
          }
          object ValueOffsetUncompressed extends ValueOffsetUncompressed {
            sealed trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetUncompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(107) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1101) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1102) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1103) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1104) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1105) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1106) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1107) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1108) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(1109) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            sealed trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetUncompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(108) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1110) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1111) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1112) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1113) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1114) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1115) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1116) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1117) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(1118) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            sealed trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetUncompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(109) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1119) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1120) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1121) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1122) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1123) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1124) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1125) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1126) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(1127) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            sealed trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetUncompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(110) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1128) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1129) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1130) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1131) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1132) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1133) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1134) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1135) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(1136) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            sealed trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetUncompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(111) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1137) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1138) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1139) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1140) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1141) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1142) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1143) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1144) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(1145) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }
        }

        sealed trait NoValue extends Value.NoValue with TimeUncompressed {
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
          object NoDeadline extends BaseEntryIdFormatA(112) with Deadline.NoDeadline with NoValue
          object DeadlineOneCompressed extends BaseEntryIdFormatA(1146) with Deadline.OneCompressed with NoValue
          object DeadlineTwoCompressed extends BaseEntryIdFormatA(1147) with Deadline.TwoCompressed with NoValue
          object DeadlineThreeCompressed extends BaseEntryIdFormatA(1148) with Deadline.ThreeCompressed with NoValue
          object DeadlineFourCompressed extends BaseEntryIdFormatA(1149) with Deadline.FourCompressed with NoValue
          object DeadlineFiveCompressed extends BaseEntryIdFormatA(1150) with Deadline.FiveCompressed with NoValue
          object DeadlineSixCompressed extends BaseEntryIdFormatA(1151) with Deadline.SixCompressed with NoValue
          object DeadlineSevenCompressed extends BaseEntryIdFormatA(1152) with Deadline.SevenCompressed with NoValue
          object DeadlineFullyCompressed extends BaseEntryIdFormatA(1153) with Deadline.FullyCompressed with NoValue
          object DeadlineUncompressed extends BaseEntryIdFormatA(1154) with Deadline.Uncompressed with NoValue
        }

        sealed trait ValueFullyCompressed extends Value.FullyCompressed with TimeUncompressed {
          override def valueOffsetOneCompressed: ValueOffset.OneCompressed = ValueFullyCompressed.ValueOffsetOneCompressed
          override def valueOffsetTwoCompressed: ValueOffset.TwoCompressed = ValueFullyCompressed.ValueOffsetTwoCompressed
          override def valueOffsetThreeCompressed: ValueOffset.ThreeCompressed = ValueFullyCompressed.ValueOffsetThreeCompressed
          override def valueOffsetUncompressed: ValueOffset.Uncompressed = ValueFullyCompressed.ValueOffsetUncompressed
          override def valueOffsetFullyCompressed: ValueOffset.FullyCompressed = ValueFullyCompressed.ValueOffsetFullyCompressed
        }
        object ValueFullyCompressed extends ValueFullyCompressed {
          sealed trait ValueOffsetOneCompressed extends ValueOffset.OneCompressed with ValueFullyCompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetOneCompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetOneCompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetOneCompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetOneCompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetOneCompressed.ValueLengthUncompressed
          }
          object ValueOffsetOneCompressed extends ValueOffsetOneCompressed {
            sealed trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetOneCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(113) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1155) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1156) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1157) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1158) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1159) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1160) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1161) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1162) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(1163) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            sealed trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetOneCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(114) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1164) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1165) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1166) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1167) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1168) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1169) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1170) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1171) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(1172) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            sealed trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetOneCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(115) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1173) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1174) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1175) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1176) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1177) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1178) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1179) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1180) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(1181) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            sealed trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetOneCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(116) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1182) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1183) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1184) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1185) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1186) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1187) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1188) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1189) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(1190) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            sealed trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetOneCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(117) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1191) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1192) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1193) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1194) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1195) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1196) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1197) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1198) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(1199) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }

          sealed trait ValueOffsetTwoCompressed extends ValueOffset.TwoCompressed with ValueFullyCompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetTwoCompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetTwoCompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetTwoCompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetTwoCompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetTwoCompressed.ValueLengthUncompressed
          }
          object ValueOffsetTwoCompressed extends ValueOffsetTwoCompressed {
            sealed trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetTwoCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(118) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1200) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1201) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1202) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1203) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1204) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1205) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1206) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1207) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(1208) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            sealed trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetTwoCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(119) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1209) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1210) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1211) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1212) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1213) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1214) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1215) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1216) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(1217) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            sealed trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetTwoCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(120) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1218) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1219) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1220) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1221) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1222) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1223) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1224) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1225) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(1226) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            sealed trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetTwoCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(121) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1227) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1228) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1229) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1230) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1231) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1232) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1233) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1234) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(1235) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            sealed trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetTwoCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(122) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1236) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1237) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1238) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1239) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1240) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1241) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1242) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1243) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(1244) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }

          sealed trait ValueOffsetThreeCompressed extends ValueOffset.ThreeCompressed with ValueFullyCompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetThreeCompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetThreeCompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetThreeCompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetThreeCompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetThreeCompressed.ValueLengthUncompressed
          }
          object ValueOffsetThreeCompressed extends ValueOffsetThreeCompressed {
            sealed trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetThreeCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(123) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1245) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1246) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1247) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1248) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1249) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1250) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1251) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1252) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(1253) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            sealed trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetThreeCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(124) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1254) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1255) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1256) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1257) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1258) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1259) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1260) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1261) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(1262) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            sealed trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetThreeCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(125) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1263) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1264) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1265) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1266) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1267) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1268) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1269) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1270) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(1271) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            sealed trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetThreeCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(126) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1272) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1273) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1274) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1275) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1276) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1277) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1278) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1279) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(1280) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            sealed trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetThreeCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(127) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1281) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1282) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1283) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1284) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1285) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1286) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1287) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1288) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(1289) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }

          sealed trait ValueOffsetFullyCompressed extends ValueOffset.FullyCompressed with ValueFullyCompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetFullyCompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetFullyCompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetFullyCompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetFullyCompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetFullyCompressed.ValueLengthUncompressed
          }
          object ValueOffsetFullyCompressed extends ValueOffsetFullyCompressed {
            sealed trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetFullyCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(128) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1290) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1291) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1292) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1293) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1294) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1295) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1296) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1297) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(1298) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            sealed trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetFullyCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(129) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1299) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1300) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1301) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1302) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1303) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1304) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1305) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1306) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(1307) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            sealed trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetFullyCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(130) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1308) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1309) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1310) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1311) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1312) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1313) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1314) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1315) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(1316) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            sealed trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetFullyCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(131) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1317) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1318) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1319) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1320) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1321) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1322) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1323) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1324) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(1325) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            sealed trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetFullyCompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(132) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1326) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1327) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1328) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1329) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1330) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1331) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1332) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1333) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(1334) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }

          sealed trait ValueOffsetUncompressed extends ValueOffset.Uncompressed with ValueFullyCompressed {
            override def valueLengthOneCompressed: ValueLength.OneCompressed = ValueOffsetUncompressed.ValueLengthOneCompressed
            override def valueLengthTwoCompressed: ValueLength.TwoCompressed = ValueOffsetUncompressed.ValueLengthTwoCompressed
            override def valueLengthThreeCompressed: ValueLength.ThreeCompressed = ValueOffsetUncompressed.ValueLengthThreeCompressed
            override def valueLengthFullyCompressed: ValueLength.FullyCompressed = ValueOffsetUncompressed.ValueLengthFullyCompressed
            override def valueLengthUncompressed: ValueLength.Uncompressed = ValueOffsetUncompressed.ValueLengthUncompressed
          }
          object ValueOffsetUncompressed extends ValueOffsetUncompressed {
            sealed trait ValueLengthOneCompressed extends ValueLength.OneCompressed with ValueOffsetUncompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(133) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1335) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1336) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1337) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1338) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1339) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1340) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1341) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1342) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(1343) with Deadline.Uncompressed with ValueLengthOneCompressed
            }

            sealed trait ValueLengthTwoCompressed extends ValueLength.TwoCompressed with ValueOffsetUncompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(134) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1344) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1345) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1346) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1347) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1348) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1349) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1350) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1351) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(1352) with Deadline.Uncompressed with ValueLengthTwoCompressed
            }

            sealed trait ValueLengthThreeCompressed extends ValueLength.ThreeCompressed with ValueOffsetUncompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(135) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1353) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1354) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1355) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1356) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1357) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1358) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1359) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1360) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(1361) with Deadline.Uncompressed with ValueLengthThreeCompressed
            }

            sealed trait ValueLengthFullyCompressed extends ValueLength.FullyCompressed with ValueOffsetUncompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(136) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1362) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1363) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1364) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1365) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1366) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1367) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1368) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1369) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(1370) with Deadline.Uncompressed with ValueLengthFullyCompressed
            }

            sealed trait ValueLengthUncompressed extends ValueLength.Uncompressed with ValueOffsetUncompressed {
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
              object NoDeadline extends BaseEntryIdFormatA(137) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1371) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1372) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1373) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1374) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1375) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1376) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1377) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1378) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(1379) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }
        }
      }

    }
  }

  def baseIds: List[BaseEntryIdFormatA] =
    SealedList.list[BaseEntryIdFormatA].sortBy(_.baseId)
}