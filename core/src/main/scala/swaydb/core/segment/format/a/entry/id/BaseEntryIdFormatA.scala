
/*
 * Copyright (c) 2020 Simer Plaha (@simerplaha)
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
import swaydb.macros.Sealed

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
              object NoDeadline extends BaseEntryIdFormatA(168) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(276) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(277) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(278) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(279) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(280) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(281) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(282) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(283) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(30) with Deadline.Uncompressed with ValueLengthOneCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(169) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(284) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(285) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(286) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(287) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(288) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(289) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(290) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(291) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(31) with Deadline.Uncompressed with ValueLengthTwoCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(170) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(292) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(293) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(294) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(295) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(296) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(297) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(298) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(299) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(32) with Deadline.Uncompressed with ValueLengthThreeCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(171) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(300) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(301) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(302) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(303) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(304) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(305) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(306) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(307) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(33) with Deadline.Uncompressed with ValueLengthFullyCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(0) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(308) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(309) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(310) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(311) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(312) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(313) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(314) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(315) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(34) with Deadline.Uncompressed with ValueLengthUncompressed
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
              object NoDeadline extends BaseEntryIdFormatA(172) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(316) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(317) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(318) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(319) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(320) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(321) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(322) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(323) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(35) with Deadline.Uncompressed with ValueLengthOneCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(173) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(324) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(325) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(326) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(327) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(328) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(329) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(330) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(331) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(36) with Deadline.Uncompressed with ValueLengthTwoCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(174) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(332) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(333) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(334) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(335) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(336) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(337) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(338) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(339) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(37) with Deadline.Uncompressed with ValueLengthThreeCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(175) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(340) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(341) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(342) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(343) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(344) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(345) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(346) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(347) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(38) with Deadline.Uncompressed with ValueLengthFullyCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(1) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(348) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(349) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(350) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(351) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(352) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(353) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(354) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(355) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(39) with Deadline.Uncompressed with ValueLengthUncompressed
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
              object NoDeadline extends BaseEntryIdFormatA(176) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(356) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(357) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(358) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(359) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(360) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(361) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(362) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(363) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(40) with Deadline.Uncompressed with ValueLengthOneCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(177) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(364) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(365) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(366) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(367) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(368) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(369) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(370) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(371) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(41) with Deadline.Uncompressed with ValueLengthTwoCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(178) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(372) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(373) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(374) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(375) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(376) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(377) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(378) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(379) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(42) with Deadline.Uncompressed with ValueLengthThreeCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(179) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(380) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(381) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(382) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(383) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(384) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(385) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(386) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(387) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(43) with Deadline.Uncompressed with ValueLengthFullyCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(2) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(388) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(389) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(390) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(391) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(392) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(393) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(394) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(395) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(44) with Deadline.Uncompressed with ValueLengthUncompressed
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
              object NoDeadline extends BaseEntryIdFormatA(180) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(396) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(397) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(398) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(399) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(400) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(401) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(402) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(403) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(45) with Deadline.Uncompressed with ValueLengthOneCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(181) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(404) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(405) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(406) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(407) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(408) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(409) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(410) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(411) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(46) with Deadline.Uncompressed with ValueLengthTwoCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(182) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(412) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(413) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(414) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(415) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(416) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(417) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(418) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(419) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(47) with Deadline.Uncompressed with ValueLengthThreeCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(183) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(420) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(421) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(422) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(423) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(424) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(425) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(426) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(427) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(48) with Deadline.Uncompressed with ValueLengthFullyCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(3) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(428) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(429) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(430) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(431) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(432) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(433) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(434) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(435) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(49) with Deadline.Uncompressed with ValueLengthUncompressed
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
          object NoDeadline extends BaseEntryIdFormatA(4) with Deadline.NoDeadline with NoValue
          object DeadlineOneCompressed extends BaseEntryIdFormatA(436) with Deadline.OneCompressed with NoValue
          object DeadlineTwoCompressed extends BaseEntryIdFormatA(437) with Deadline.TwoCompressed with NoValue
          object DeadlineThreeCompressed extends BaseEntryIdFormatA(438) with Deadline.ThreeCompressed with NoValue
          object DeadlineFourCompressed extends BaseEntryIdFormatA(439) with Deadline.FourCompressed with NoValue
          object DeadlineFiveCompressed extends BaseEntryIdFormatA(440) with Deadline.FiveCompressed with NoValue
          object DeadlineSixCompressed extends BaseEntryIdFormatA(441) with Deadline.SixCompressed with NoValue
          object DeadlineSevenCompressed extends BaseEntryIdFormatA(442) with Deadline.SevenCompressed with NoValue
          object DeadlineFullyCompressed extends BaseEntryIdFormatA(443) with Deadline.FullyCompressed with NoValue
          object DeadlineUncompressed extends BaseEntryIdFormatA(50) with Deadline.Uncompressed with NoValue
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
              object NoDeadline extends BaseEntryIdFormatA(184) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(444) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(445) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(446) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(447) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(448) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(449) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(450) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(451) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(51) with Deadline.Uncompressed with ValueLengthOneCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(185) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(452) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(453) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(454) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(455) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(456) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(457) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(458) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(459) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(52) with Deadline.Uncompressed with ValueLengthTwoCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(186) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(460) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(461) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(462) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(463) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(464) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(465) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(466) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(467) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(53) with Deadline.Uncompressed with ValueLengthThreeCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(187) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(468) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(469) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(470) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(471) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(472) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(473) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(474) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(475) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(54) with Deadline.Uncompressed with ValueLengthFullyCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(5) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(476) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(477) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(478) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(479) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(480) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(481) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(482) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(483) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(55) with Deadline.Uncompressed with ValueLengthUncompressed
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
              object NoDeadline extends BaseEntryIdFormatA(188) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(484) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(485) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(486) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(487) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(488) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(489) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(490) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(491) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(56) with Deadline.Uncompressed with ValueLengthOneCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(189) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(492) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(493) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(494) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(495) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(496) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(497) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(498) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(499) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(57) with Deadline.Uncompressed with ValueLengthTwoCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(190) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(500) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(501) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(502) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(503) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(504) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(505) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(506) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(507) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(58) with Deadline.Uncompressed with ValueLengthThreeCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(191) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(508) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(509) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(510) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(511) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(512) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(513) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(514) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(515) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(59) with Deadline.Uncompressed with ValueLengthFullyCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(6) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(516) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(517) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(518) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(519) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(520) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(521) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(522) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(523) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(60) with Deadline.Uncompressed with ValueLengthUncompressed
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
              object NoDeadline extends BaseEntryIdFormatA(192) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(524) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(525) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(526) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(527) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(528) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(529) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(530) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(531) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(61) with Deadline.Uncompressed with ValueLengthOneCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(193) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(532) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(533) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(534) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(535) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(536) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(537) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(538) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(539) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(62) with Deadline.Uncompressed with ValueLengthTwoCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(194) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(540) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(541) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(542) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(543) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(544) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(545) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(546) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(547) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(63) with Deadline.Uncompressed with ValueLengthThreeCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(195) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(548) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(549) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(550) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(551) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(552) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(553) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(554) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(555) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(64) with Deadline.Uncompressed with ValueLengthFullyCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(7) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(556) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(557) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(558) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(559) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(560) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(561) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(562) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(563) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(65) with Deadline.Uncompressed with ValueLengthUncompressed
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
              object NoDeadline extends BaseEntryIdFormatA(196) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(564) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(565) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(566) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(567) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(568) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(569) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(570) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(571) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(66) with Deadline.Uncompressed with ValueLengthOneCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(197) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(572) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(573) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(574) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(575) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(576) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(577) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(578) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(579) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(67) with Deadline.Uncompressed with ValueLengthTwoCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(198) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(580) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(581) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(582) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(583) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(584) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(585) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(586) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(587) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(68) with Deadline.Uncompressed with ValueLengthThreeCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(199) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(588) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(589) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(590) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(591) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(592) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(593) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(594) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(595) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(69) with Deadline.Uncompressed with ValueLengthFullyCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(8) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(596) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(597) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(598) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(599) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(600) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(601) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(602) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(603) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(70) with Deadline.Uncompressed with ValueLengthUncompressed
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
              object NoDeadline extends BaseEntryIdFormatA(200) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(604) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(605) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(606) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(607) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(608) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(609) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(610) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(611) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(71) with Deadline.Uncompressed with ValueLengthOneCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(201) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(612) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(613) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(614) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(615) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(616) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(617) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(618) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(619) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(72) with Deadline.Uncompressed with ValueLengthTwoCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(202) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(620) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(621) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(622) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(623) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(624) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(625) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(626) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(627) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(73) with Deadline.Uncompressed with ValueLengthThreeCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(203) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(628) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(629) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(630) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(631) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(632) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(633) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(634) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(635) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(74) with Deadline.Uncompressed with ValueLengthFullyCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(9) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(636) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(637) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(638) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(639) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(640) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(641) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(642) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(643) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(75) with Deadline.Uncompressed with ValueLengthUncompressed
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
              object NoDeadline extends BaseEntryIdFormatA(204) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(644) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(645) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(646) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(647) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(648) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(649) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(650) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(651) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(76) with Deadline.Uncompressed with ValueLengthOneCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(205) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(652) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(653) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(654) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(655) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(656) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(657) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(658) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(659) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(77) with Deadline.Uncompressed with ValueLengthTwoCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(206) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(660) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(661) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(662) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(663) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(664) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(665) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(666) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(667) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(78) with Deadline.Uncompressed with ValueLengthThreeCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(207) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(668) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(669) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(670) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(671) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(672) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(673) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(674) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(675) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(79) with Deadline.Uncompressed with ValueLengthFullyCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(10) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(676) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(677) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(678) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(679) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(680) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(681) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(682) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(683) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(80) with Deadline.Uncompressed with ValueLengthUncompressed
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
              object NoDeadline extends BaseEntryIdFormatA(208) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(684) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(685) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(686) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(687) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(688) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(689) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(690) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(691) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(81) with Deadline.Uncompressed with ValueLengthOneCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(209) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(692) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(693) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(694) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(695) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(696) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(697) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(698) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(699) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(82) with Deadline.Uncompressed with ValueLengthTwoCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(210) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(700) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(701) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(702) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(703) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(704) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(705) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(706) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(707) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(83) with Deadline.Uncompressed with ValueLengthThreeCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(211) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(708) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(709) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(710) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(711) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(712) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(713) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(714) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(715) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(84) with Deadline.Uncompressed with ValueLengthFullyCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(11) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(716) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(717) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(718) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(719) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(720) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(721) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(722) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(723) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(85) with Deadline.Uncompressed with ValueLengthUncompressed
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
              object NoDeadline extends BaseEntryIdFormatA(212) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(724) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(725) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(726) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(727) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(728) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(729) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(730) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(731) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(86) with Deadline.Uncompressed with ValueLengthOneCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(213) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(732) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(733) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(734) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(735) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(736) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(737) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(738) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(739) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(87) with Deadline.Uncompressed with ValueLengthTwoCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(214) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(740) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(741) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(742) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(743) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(744) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(745) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(746) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(747) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(88) with Deadline.Uncompressed with ValueLengthThreeCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(215) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(748) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(749) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(750) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(751) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(752) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(753) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(754) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(755) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(89) with Deadline.Uncompressed with ValueLengthFullyCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(12) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(756) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(757) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(758) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(759) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(760) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(761) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(762) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(763) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(90) with Deadline.Uncompressed with ValueLengthUncompressed
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
              object NoDeadline extends BaseEntryIdFormatA(216) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(764) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(765) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(766) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(767) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(768) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(769) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(770) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(771) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(91) with Deadline.Uncompressed with ValueLengthOneCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(217) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(772) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(773) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(774) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(775) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(776) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(777) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(778) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(779) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(92) with Deadline.Uncompressed with ValueLengthTwoCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(218) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(780) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(781) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(782) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(783) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(784) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(785) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(786) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(787) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(93) with Deadline.Uncompressed with ValueLengthThreeCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(219) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(788) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(789) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(790) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(791) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(792) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(793) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(794) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(795) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(94) with Deadline.Uncompressed with ValueLengthFullyCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(13) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(796) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(797) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(798) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(799) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(800) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(801) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(802) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(803) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(95) with Deadline.Uncompressed with ValueLengthUncompressed
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
          object NoDeadline extends BaseEntryIdFormatA(14) with Deadline.NoDeadline with NoValue
          object DeadlineOneCompressed extends BaseEntryIdFormatA(804) with Deadline.OneCompressed with NoValue
          object DeadlineTwoCompressed extends BaseEntryIdFormatA(805) with Deadline.TwoCompressed with NoValue
          object DeadlineThreeCompressed extends BaseEntryIdFormatA(806) with Deadline.ThreeCompressed with NoValue
          object DeadlineFourCompressed extends BaseEntryIdFormatA(807) with Deadline.FourCompressed with NoValue
          object DeadlineFiveCompressed extends BaseEntryIdFormatA(808) with Deadline.FiveCompressed with NoValue
          object DeadlineSixCompressed extends BaseEntryIdFormatA(809) with Deadline.SixCompressed with NoValue
          object DeadlineSevenCompressed extends BaseEntryIdFormatA(810) with Deadline.SevenCompressed with NoValue
          object DeadlineFullyCompressed extends BaseEntryIdFormatA(811) with Deadline.FullyCompressed with NoValue
          object DeadlineUncompressed extends BaseEntryIdFormatA(96) with Deadline.Uncompressed with NoValue
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
              object NoDeadline extends BaseEntryIdFormatA(220) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(812) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(813) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(814) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(815) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(816) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(817) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(818) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(819) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(97) with Deadline.Uncompressed with ValueLengthOneCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(221) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(820) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(821) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(822) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(823) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(824) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(825) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(826) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(827) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(98) with Deadline.Uncompressed with ValueLengthTwoCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(222) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(828) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(829) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(830) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(831) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(832) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(833) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(834) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(835) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(99) with Deadline.Uncompressed with ValueLengthThreeCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(223) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(836) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(837) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(838) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(839) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(840) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(841) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(842) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(843) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(100) with Deadline.Uncompressed with ValueLengthFullyCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(15) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(844) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(845) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(846) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(847) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(848) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(849) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(850) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(851) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(101) with Deadline.Uncompressed with ValueLengthUncompressed
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
              object NoDeadline extends BaseEntryIdFormatA(224) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(852) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(853) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(854) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(855) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(856) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(857) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(858) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(859) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(102) with Deadline.Uncompressed with ValueLengthOneCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(225) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(860) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(861) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(862) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(863) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(864) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(865) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(866) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(867) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(103) with Deadline.Uncompressed with ValueLengthTwoCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(226) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(868) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(869) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(870) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(871) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(872) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(873) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(874) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(875) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(104) with Deadline.Uncompressed with ValueLengthThreeCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(227) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(876) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(877) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(878) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(879) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(880) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(881) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(882) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(883) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(105) with Deadline.Uncompressed with ValueLengthFullyCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(16) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(884) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(885) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(886) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(887) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(888) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(889) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(890) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(891) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(106) with Deadline.Uncompressed with ValueLengthUncompressed
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
              object NoDeadline extends BaseEntryIdFormatA(228) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(892) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(893) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(894) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(895) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(896) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(897) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(898) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(899) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(107) with Deadline.Uncompressed with ValueLengthOneCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(229) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(900) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(901) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(902) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(903) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(904) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(905) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(906) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(907) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(108) with Deadline.Uncompressed with ValueLengthTwoCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(230) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(908) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(909) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(910) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(911) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(912) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(913) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(914) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(915) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(109) with Deadline.Uncompressed with ValueLengthThreeCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(231) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(916) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(917) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(918) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(919) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(920) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(921) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(922) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(923) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(110) with Deadline.Uncompressed with ValueLengthFullyCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(17) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(924) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(925) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(926) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(927) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(928) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(929) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(930) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(931) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(111) with Deadline.Uncompressed with ValueLengthUncompressed
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
              object NoDeadline extends BaseEntryIdFormatA(232) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(932) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(933) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(934) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(935) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(936) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(937) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(938) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(939) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(112) with Deadline.Uncompressed with ValueLengthOneCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(233) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(940) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(941) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(942) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(943) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(944) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(945) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(946) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(947) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(113) with Deadline.Uncompressed with ValueLengthTwoCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(234) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(948) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(949) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(950) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(951) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(952) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(953) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(954) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(955) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(114) with Deadline.Uncompressed with ValueLengthThreeCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(235) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(956) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(957) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(958) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(959) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(960) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(961) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(962) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(963) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(115) with Deadline.Uncompressed with ValueLengthFullyCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(18) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(964) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(965) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(966) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(967) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(968) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(969) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(970) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(971) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(116) with Deadline.Uncompressed with ValueLengthUncompressed
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
              object NoDeadline extends BaseEntryIdFormatA(236) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(972) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(973) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(974) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(975) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(976) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(977) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(978) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(979) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(117) with Deadline.Uncompressed with ValueLengthOneCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(237) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(980) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(981) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(982) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(983) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(984) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(985) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(986) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(987) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(118) with Deadline.Uncompressed with ValueLengthTwoCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(238) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(988) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(989) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(990) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(991) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(992) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(993) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(994) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(995) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(119) with Deadline.Uncompressed with ValueLengthThreeCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(239) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(996) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(997) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(998) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(999) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1000) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1001) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1002) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1003) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(120) with Deadline.Uncompressed with ValueLengthFullyCompressed
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
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1004) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1005) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1006) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1007) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1008) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1009) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1010) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1011) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(121) with Deadline.Uncompressed with ValueLengthUncompressed
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
              object NoDeadline extends BaseEntryIdFormatA(240) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1012) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1013) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1014) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1015) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1016) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1017) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1018) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1019) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(122) with Deadline.Uncompressed with ValueLengthOneCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(241) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1020) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1021) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1022) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1023) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1024) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1025) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1026) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1027) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(123) with Deadline.Uncompressed with ValueLengthTwoCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(242) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1028) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1029) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1030) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1031) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1032) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1033) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1034) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1035) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(124) with Deadline.Uncompressed with ValueLengthThreeCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(243) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1036) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1037) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1038) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1039) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1040) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1041) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1042) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1043) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(125) with Deadline.Uncompressed with ValueLengthFullyCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(20) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1044) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1045) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1046) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1047) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1048) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1049) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1050) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1051) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(126) with Deadline.Uncompressed with ValueLengthUncompressed
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
              object NoDeadline extends BaseEntryIdFormatA(244) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1052) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1053) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1054) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1055) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1056) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1057) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1058) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1059) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(127) with Deadline.Uncompressed with ValueLengthOneCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(245) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1060) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1061) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1062) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1063) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1064) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1065) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1066) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1067) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(128) with Deadline.Uncompressed with ValueLengthTwoCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(246) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1068) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1069) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1070) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1071) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1072) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1073) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1074) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1075) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(129) with Deadline.Uncompressed with ValueLengthThreeCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(247) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1076) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1077) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1078) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1079) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1080) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1081) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1082) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1083) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(130) with Deadline.Uncompressed with ValueLengthFullyCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(21) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1084) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1085) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1086) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1087) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1088) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1089) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1090) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1091) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(131) with Deadline.Uncompressed with ValueLengthUncompressed
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
              object NoDeadline extends BaseEntryIdFormatA(248) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1092) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1093) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1094) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1095) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1096) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1097) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1098) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1099) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(132) with Deadline.Uncompressed with ValueLengthOneCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(249) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1100) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1101) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1102) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1103) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1104) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1105) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1106) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1107) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(133) with Deadline.Uncompressed with ValueLengthTwoCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(250) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1108) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1109) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1110) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1111) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1112) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1113) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1114) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1115) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(134) with Deadline.Uncompressed with ValueLengthThreeCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(251) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1116) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1117) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1118) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1119) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1120) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1121) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1122) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1123) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(135) with Deadline.Uncompressed with ValueLengthFullyCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(22) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1124) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1125) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1126) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1127) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1128) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1129) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1130) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1131) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(136) with Deadline.Uncompressed with ValueLengthUncompressed
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
              object NoDeadline extends BaseEntryIdFormatA(252) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1132) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1133) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1134) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1135) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1136) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1137) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1138) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1139) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(137) with Deadline.Uncompressed with ValueLengthOneCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(253) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1140) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1141) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1142) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1143) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1144) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1145) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1146) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1147) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(138) with Deadline.Uncompressed with ValueLengthTwoCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(254) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1148) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1149) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1150) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1151) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1152) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1153) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1154) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1155) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(139) with Deadline.Uncompressed with ValueLengthThreeCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(255) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1156) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1157) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1158) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1159) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1160) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1161) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1162) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1163) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(140) with Deadline.Uncompressed with ValueLengthFullyCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(23) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1164) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1165) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1166) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1167) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1168) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1169) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1170) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1171) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(141) with Deadline.Uncompressed with ValueLengthUncompressed
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
          object NoDeadline extends BaseEntryIdFormatA(24) with Deadline.NoDeadline with NoValue
          object DeadlineOneCompressed extends BaseEntryIdFormatA(1172) with Deadline.OneCompressed with NoValue
          object DeadlineTwoCompressed extends BaseEntryIdFormatA(1173) with Deadline.TwoCompressed with NoValue
          object DeadlineThreeCompressed extends BaseEntryIdFormatA(1174) with Deadline.ThreeCompressed with NoValue
          object DeadlineFourCompressed extends BaseEntryIdFormatA(1175) with Deadline.FourCompressed with NoValue
          object DeadlineFiveCompressed extends BaseEntryIdFormatA(1176) with Deadline.FiveCompressed with NoValue
          object DeadlineSixCompressed extends BaseEntryIdFormatA(1177) with Deadline.SixCompressed with NoValue
          object DeadlineSevenCompressed extends BaseEntryIdFormatA(1178) with Deadline.SevenCompressed with NoValue
          object DeadlineFullyCompressed extends BaseEntryIdFormatA(1179) with Deadline.FullyCompressed with NoValue
          object DeadlineUncompressed extends BaseEntryIdFormatA(142) with Deadline.Uncompressed with NoValue
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
              object NoDeadline extends BaseEntryIdFormatA(256) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1180) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1181) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1182) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1183) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1184) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1185) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1186) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1187) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(143) with Deadline.Uncompressed with ValueLengthOneCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(257) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1188) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1189) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1190) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1191) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1192) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1193) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1194) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1195) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(144) with Deadline.Uncompressed with ValueLengthTwoCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(258) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1196) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1197) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1198) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1199) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1200) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1201) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1202) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1203) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(145) with Deadline.Uncompressed with ValueLengthThreeCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(259) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1204) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1205) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1206) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1207) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1208) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1209) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1210) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1211) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(146) with Deadline.Uncompressed with ValueLengthFullyCompressed
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
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1212) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1213) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1214) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1215) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1216) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1217) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1218) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1219) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(147) with Deadline.Uncompressed with ValueLengthUncompressed
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
              object NoDeadline extends BaseEntryIdFormatA(260) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1220) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1221) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1222) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1223) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1224) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1225) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1226) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1227) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(148) with Deadline.Uncompressed with ValueLengthOneCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(261) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1228) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1229) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1230) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1231) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1232) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1233) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1234) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1235) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(149) with Deadline.Uncompressed with ValueLengthTwoCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(262) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1236) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1237) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1238) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1239) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1240) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1241) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1242) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1243) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(150) with Deadline.Uncompressed with ValueLengthThreeCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(263) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1244) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1245) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1246) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1247) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1248) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1249) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1250) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1251) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(151) with Deadline.Uncompressed with ValueLengthFullyCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(26) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1252) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1253) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1254) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1255) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1256) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1257) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1258) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1259) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(152) with Deadline.Uncompressed with ValueLengthUncompressed
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
              object NoDeadline extends BaseEntryIdFormatA(264) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1260) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1261) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1262) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1263) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1264) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1265) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1266) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1267) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(153) with Deadline.Uncompressed with ValueLengthOneCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(265) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1268) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1269) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1270) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1271) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1272) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1273) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1274) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1275) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(154) with Deadline.Uncompressed with ValueLengthTwoCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(266) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1276) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1277) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1278) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1279) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1280) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1281) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1282) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1283) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(155) with Deadline.Uncompressed with ValueLengthThreeCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(267) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1284) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1285) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1286) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1287) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1288) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1289) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1290) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1291) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(156) with Deadline.Uncompressed with ValueLengthFullyCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(27) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1292) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1293) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1294) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1295) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1296) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1297) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1298) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1299) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(157) with Deadline.Uncompressed with ValueLengthUncompressed
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
              object NoDeadline extends BaseEntryIdFormatA(268) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1300) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1301) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1302) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1303) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1304) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1305) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1306) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1307) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(158) with Deadline.Uncompressed with ValueLengthOneCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(269) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1308) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1309) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1310) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1311) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1312) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1313) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1314) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1315) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(159) with Deadline.Uncompressed with ValueLengthTwoCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(270) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1316) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1317) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1318) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1319) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1320) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1321) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1322) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1323) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(160) with Deadline.Uncompressed with ValueLengthThreeCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(271) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1324) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1325) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1326) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1327) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1328) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1329) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1330) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1331) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(161) with Deadline.Uncompressed with ValueLengthFullyCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(28) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1332) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1333) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1334) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1335) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1336) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1337) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1338) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1339) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(162) with Deadline.Uncompressed with ValueLengthUncompressed
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
              object NoDeadline extends BaseEntryIdFormatA(272) with Deadline.NoDeadline with ValueLengthOneCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1340) with Deadline.OneCompressed with ValueLengthOneCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1341) with Deadline.TwoCompressed with ValueLengthOneCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1342) with Deadline.ThreeCompressed with ValueLengthOneCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1343) with Deadline.FourCompressed with ValueLengthOneCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1344) with Deadline.FiveCompressed with ValueLengthOneCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1345) with Deadline.SixCompressed with ValueLengthOneCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1346) with Deadline.SevenCompressed with ValueLengthOneCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1347) with Deadline.FullyCompressed with ValueLengthOneCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(163) with Deadline.Uncompressed with ValueLengthOneCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(273) with Deadline.NoDeadline with ValueLengthTwoCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1348) with Deadline.OneCompressed with ValueLengthTwoCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1349) with Deadline.TwoCompressed with ValueLengthTwoCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1350) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1351) with Deadline.FourCompressed with ValueLengthTwoCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1352) with Deadline.FiveCompressed with ValueLengthTwoCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1353) with Deadline.SixCompressed with ValueLengthTwoCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1354) with Deadline.SevenCompressed with ValueLengthTwoCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1355) with Deadline.FullyCompressed with ValueLengthTwoCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(164) with Deadline.Uncompressed with ValueLengthTwoCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(274) with Deadline.NoDeadline with ValueLengthThreeCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1356) with Deadline.OneCompressed with ValueLengthThreeCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1357) with Deadline.TwoCompressed with ValueLengthThreeCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1358) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1359) with Deadline.FourCompressed with ValueLengthThreeCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1360) with Deadline.FiveCompressed with ValueLengthThreeCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1361) with Deadline.SixCompressed with ValueLengthThreeCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1362) with Deadline.SevenCompressed with ValueLengthThreeCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1363) with Deadline.FullyCompressed with ValueLengthThreeCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(165) with Deadline.Uncompressed with ValueLengthThreeCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(275) with Deadline.NoDeadline with ValueLengthFullyCompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1364) with Deadline.OneCompressed with ValueLengthFullyCompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1365) with Deadline.TwoCompressed with ValueLengthFullyCompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1366) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1367) with Deadline.FourCompressed with ValueLengthFullyCompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1368) with Deadline.FiveCompressed with ValueLengthFullyCompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1369) with Deadline.SixCompressed with ValueLengthFullyCompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1370) with Deadline.SevenCompressed with ValueLengthFullyCompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1371) with Deadline.FullyCompressed with ValueLengthFullyCompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(166) with Deadline.Uncompressed with ValueLengthFullyCompressed
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
              object NoDeadline extends BaseEntryIdFormatA(29) with Deadline.NoDeadline with ValueLengthUncompressed
              object DeadlineOneCompressed extends BaseEntryIdFormatA(1372) with Deadline.OneCompressed with ValueLengthUncompressed
              object DeadlineTwoCompressed extends BaseEntryIdFormatA(1373) with Deadline.TwoCompressed with ValueLengthUncompressed
              object DeadlineThreeCompressed extends BaseEntryIdFormatA(1374) with Deadline.ThreeCompressed with ValueLengthUncompressed
              object DeadlineFourCompressed extends BaseEntryIdFormatA(1375) with Deadline.FourCompressed with ValueLengthUncompressed
              object DeadlineFiveCompressed extends BaseEntryIdFormatA(1376) with Deadline.FiveCompressed with ValueLengthUncompressed
              object DeadlineSixCompressed extends BaseEntryIdFormatA(1377) with Deadline.SixCompressed with ValueLengthUncompressed
              object DeadlineSevenCompressed extends BaseEntryIdFormatA(1378) with Deadline.SevenCompressed with ValueLengthUncompressed
              object DeadlineFullyCompressed extends BaseEntryIdFormatA(1379) with Deadline.FullyCompressed with ValueLengthUncompressed
              object DeadlineUncompressed extends BaseEntryIdFormatA(167) with Deadline.Uncompressed with ValueLengthUncompressed
            }
          }
        }
      }
    }
  }

  def baseIds: List[BaseEntryIdFormatA] =
    Sealed.list[BaseEntryIdFormatA].sortBy(_.baseId)
}