
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
sealed abstract class RangeKeyPartiallyCompressedEntryId(override val id: Int) extends EntryId(id)
object RangeKeyPartiallyCompressedEntryId {

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
      case object NoDeadline extends RangeKeyPartiallyCompressedEntryId(1356) with Deadline.NoDeadline with NoValue
      case object DeadlineOneCompressed extends RangeKeyPartiallyCompressedEntryId(1357) with Deadline.OneCompressed with NoValue
      case object DeadlineTwoCompressed extends RangeKeyPartiallyCompressedEntryId(1358) with Deadline.TwoCompressed with NoValue
      case object DeadlineThreeCompressed extends RangeKeyPartiallyCompressedEntryId(1359) with Deadline.ThreeCompressed with NoValue
      case object DeadlineFourCompressed extends RangeKeyPartiallyCompressedEntryId(1360) with Deadline.FourCompressed with NoValue
      case object DeadlineFiveCompressed extends RangeKeyPartiallyCompressedEntryId(1361) with Deadline.FiveCompressed with NoValue
      case object DeadlineSixCompressed extends RangeKeyPartiallyCompressedEntryId(1362) with Deadline.SixCompressed with NoValue
      case object DeadlineSevenCompressed extends RangeKeyPartiallyCompressedEntryId(1363) with Deadline.SevenCompressed with NoValue
      case object DeadlineFullyCompressed extends RangeKeyPartiallyCompressedEntryId(1364) with Deadline.FullyCompressed with NoValue
      case object DeadlineUncompressed extends RangeKeyPartiallyCompressedEntryId(1365) with Deadline.Uncompressed with NoValue
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
      case object NoDeadline extends RangeKeyPartiallyCompressedEntryId(1366) with Deadline.NoDeadline with ValueFullyCompressed
      case object DeadlineOneCompressed extends RangeKeyPartiallyCompressedEntryId(1367) with Deadline.OneCompressed with ValueFullyCompressed
      case object DeadlineTwoCompressed extends RangeKeyPartiallyCompressedEntryId(1368) with Deadline.TwoCompressed with ValueFullyCompressed
      case object DeadlineThreeCompressed extends RangeKeyPartiallyCompressedEntryId(1369) with Deadline.ThreeCompressed with ValueFullyCompressed
      case object DeadlineFourCompressed extends RangeKeyPartiallyCompressedEntryId(1370) with Deadline.FourCompressed with ValueFullyCompressed
      case object DeadlineFiveCompressed extends RangeKeyPartiallyCompressedEntryId(1371) with Deadline.FiveCompressed with ValueFullyCompressed
      case object DeadlineSixCompressed extends RangeKeyPartiallyCompressedEntryId(1372) with Deadline.SixCompressed with ValueFullyCompressed
      case object DeadlineSevenCompressed extends RangeKeyPartiallyCompressedEntryId(1373) with Deadline.SevenCompressed with ValueFullyCompressed
      case object DeadlineFullyCompressed extends RangeKeyPartiallyCompressedEntryId(1374) with Deadline.FullyCompressed with ValueFullyCompressed
      case object DeadlineUncompressed extends RangeKeyPartiallyCompressedEntryId(1375) with Deadline.Uncompressed with ValueFullyCompressed
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
          case object NoDeadline extends RangeKeyPartiallyCompressedEntryId(1376) with Deadline.NoDeadline with ValueLengthOneCompressed
          case object DeadlineOneCompressed extends RangeKeyPartiallyCompressedEntryId(1377) with Deadline.OneCompressed with ValueLengthOneCompressed
          case object DeadlineTwoCompressed extends RangeKeyPartiallyCompressedEntryId(1378) with Deadline.TwoCompressed with ValueLengthOneCompressed
          case object DeadlineThreeCompressed extends RangeKeyPartiallyCompressedEntryId(1379) with Deadline.ThreeCompressed with ValueLengthOneCompressed
          case object DeadlineFourCompressed extends RangeKeyPartiallyCompressedEntryId(1380) with Deadline.FourCompressed with ValueLengthOneCompressed
          case object DeadlineFiveCompressed extends RangeKeyPartiallyCompressedEntryId(1381) with Deadline.FiveCompressed with ValueLengthOneCompressed
          case object DeadlineSixCompressed extends RangeKeyPartiallyCompressedEntryId(1382) with Deadline.SixCompressed with ValueLengthOneCompressed
          case object DeadlineSevenCompressed extends RangeKeyPartiallyCompressedEntryId(1383) with Deadline.SevenCompressed with ValueLengthOneCompressed
          case object DeadlineFullyCompressed extends RangeKeyPartiallyCompressedEntryId(1384) with Deadline.FullyCompressed with ValueLengthOneCompressed
          case object DeadlineUncompressed extends RangeKeyPartiallyCompressedEntryId(1385) with Deadline.Uncompressed with ValueLengthOneCompressed
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
          case object NoDeadline extends RangeKeyPartiallyCompressedEntryId(1386) with Deadline.NoDeadline with ValueLengthTwoCompressed
          case object DeadlineOneCompressed extends RangeKeyPartiallyCompressedEntryId(1387) with Deadline.OneCompressed with ValueLengthTwoCompressed
          case object DeadlineTwoCompressed extends RangeKeyPartiallyCompressedEntryId(1388) with Deadline.TwoCompressed with ValueLengthTwoCompressed
          case object DeadlineThreeCompressed extends RangeKeyPartiallyCompressedEntryId(1389) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
          case object DeadlineFourCompressed extends RangeKeyPartiallyCompressedEntryId(1390) with Deadline.FourCompressed with ValueLengthTwoCompressed
          case object DeadlineFiveCompressed extends RangeKeyPartiallyCompressedEntryId(1391) with Deadline.FiveCompressed with ValueLengthTwoCompressed
          case object DeadlineSixCompressed extends RangeKeyPartiallyCompressedEntryId(1392) with Deadline.SixCompressed with ValueLengthTwoCompressed
          case object DeadlineSevenCompressed extends RangeKeyPartiallyCompressedEntryId(1393) with Deadline.SevenCompressed with ValueLengthTwoCompressed
          case object DeadlineFullyCompressed extends RangeKeyPartiallyCompressedEntryId(1394) with Deadline.FullyCompressed with ValueLengthTwoCompressed
          case object DeadlineUncompressed extends RangeKeyPartiallyCompressedEntryId(1395) with Deadline.Uncompressed with ValueLengthTwoCompressed
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
          case object NoDeadline extends RangeKeyPartiallyCompressedEntryId(1396) with Deadline.NoDeadline with ValueLengthThreeCompressed
          case object DeadlineOneCompressed extends RangeKeyPartiallyCompressedEntryId(1397) with Deadline.OneCompressed with ValueLengthThreeCompressed
          case object DeadlineTwoCompressed extends RangeKeyPartiallyCompressedEntryId(1398) with Deadline.TwoCompressed with ValueLengthThreeCompressed
          case object DeadlineThreeCompressed extends RangeKeyPartiallyCompressedEntryId(1399) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
          case object DeadlineFourCompressed extends RangeKeyPartiallyCompressedEntryId(1400) with Deadline.FourCompressed with ValueLengthThreeCompressed
          case object DeadlineFiveCompressed extends RangeKeyPartiallyCompressedEntryId(1401) with Deadline.FiveCompressed with ValueLengthThreeCompressed
          case object DeadlineSixCompressed extends RangeKeyPartiallyCompressedEntryId(1402) with Deadline.SixCompressed with ValueLengthThreeCompressed
          case object DeadlineSevenCompressed extends RangeKeyPartiallyCompressedEntryId(1403) with Deadline.SevenCompressed with ValueLengthThreeCompressed
          case object DeadlineFullyCompressed extends RangeKeyPartiallyCompressedEntryId(1404) with Deadline.FullyCompressed with ValueLengthThreeCompressed
          case object DeadlineUncompressed extends RangeKeyPartiallyCompressedEntryId(1405) with Deadline.Uncompressed with ValueLengthThreeCompressed
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
          case object NoDeadline extends RangeKeyPartiallyCompressedEntryId(1406) with Deadline.NoDeadline with ValueLengthFullyCompressed
          case object DeadlineOneCompressed extends RangeKeyPartiallyCompressedEntryId(1407) with Deadline.OneCompressed with ValueLengthFullyCompressed
          case object DeadlineTwoCompressed extends RangeKeyPartiallyCompressedEntryId(1408) with Deadline.TwoCompressed with ValueLengthFullyCompressed
          case object DeadlineThreeCompressed extends RangeKeyPartiallyCompressedEntryId(1409) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
          case object DeadlineFourCompressed extends RangeKeyPartiallyCompressedEntryId(1410) with Deadline.FourCompressed with ValueLengthFullyCompressed
          case object DeadlineFiveCompressed extends RangeKeyPartiallyCompressedEntryId(1411) with Deadline.FiveCompressed with ValueLengthFullyCompressed
          case object DeadlineSixCompressed extends RangeKeyPartiallyCompressedEntryId(1412) with Deadline.SixCompressed with ValueLengthFullyCompressed
          case object DeadlineSevenCompressed extends RangeKeyPartiallyCompressedEntryId(1413) with Deadline.SevenCompressed with ValueLengthFullyCompressed
          case object DeadlineFullyCompressed extends RangeKeyPartiallyCompressedEntryId(1414) with Deadline.FullyCompressed with ValueLengthFullyCompressed
          case object DeadlineUncompressed extends RangeKeyPartiallyCompressedEntryId(1415) with Deadline.Uncompressed with ValueLengthFullyCompressed
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
          case object NoDeadline extends RangeKeyPartiallyCompressedEntryId(1416) with Deadline.NoDeadline with ValueLengthUncompressed
          case object DeadlineOneCompressed extends RangeKeyPartiallyCompressedEntryId(1417) with Deadline.OneCompressed with ValueLengthUncompressed
          case object DeadlineTwoCompressed extends RangeKeyPartiallyCompressedEntryId(1418) with Deadline.TwoCompressed with ValueLengthUncompressed
          case object DeadlineThreeCompressed extends RangeKeyPartiallyCompressedEntryId(1419) with Deadline.ThreeCompressed with ValueLengthUncompressed
          case object DeadlineFourCompressed extends RangeKeyPartiallyCompressedEntryId(1420) with Deadline.FourCompressed with ValueLengthUncompressed
          case object DeadlineFiveCompressed extends RangeKeyPartiallyCompressedEntryId(1421) with Deadline.FiveCompressed with ValueLengthUncompressed
          case object DeadlineSixCompressed extends RangeKeyPartiallyCompressedEntryId(1422) with Deadline.SixCompressed with ValueLengthUncompressed
          case object DeadlineSevenCompressed extends RangeKeyPartiallyCompressedEntryId(1423) with Deadline.SevenCompressed with ValueLengthUncompressed
          case object DeadlineFullyCompressed extends RangeKeyPartiallyCompressedEntryId(1424) with Deadline.FullyCompressed with ValueLengthUncompressed
          case object DeadlineUncompressed extends RangeKeyPartiallyCompressedEntryId(1425) with Deadline.Uncompressed with ValueLengthUncompressed
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
          case object NoDeadline extends RangeKeyPartiallyCompressedEntryId(1426) with Deadline.NoDeadline with ValueLengthOneCompressed
          case object DeadlineOneCompressed extends RangeKeyPartiallyCompressedEntryId(1427) with Deadline.OneCompressed with ValueLengthOneCompressed
          case object DeadlineTwoCompressed extends RangeKeyPartiallyCompressedEntryId(1428) with Deadline.TwoCompressed with ValueLengthOneCompressed
          case object DeadlineThreeCompressed extends RangeKeyPartiallyCompressedEntryId(1429) with Deadline.ThreeCompressed with ValueLengthOneCompressed
          case object DeadlineFourCompressed extends RangeKeyPartiallyCompressedEntryId(1430) with Deadline.FourCompressed with ValueLengthOneCompressed
          case object DeadlineFiveCompressed extends RangeKeyPartiallyCompressedEntryId(1431) with Deadline.FiveCompressed with ValueLengthOneCompressed
          case object DeadlineSixCompressed extends RangeKeyPartiallyCompressedEntryId(1432) with Deadline.SixCompressed with ValueLengthOneCompressed
          case object DeadlineSevenCompressed extends RangeKeyPartiallyCompressedEntryId(1433) with Deadline.SevenCompressed with ValueLengthOneCompressed
          case object DeadlineFullyCompressed extends RangeKeyPartiallyCompressedEntryId(1434) with Deadline.FullyCompressed with ValueLengthOneCompressed
          case object DeadlineUncompressed extends RangeKeyPartiallyCompressedEntryId(1435) with Deadline.Uncompressed with ValueLengthOneCompressed
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
          case object NoDeadline extends RangeKeyPartiallyCompressedEntryId(1436) with Deadline.NoDeadline with ValueLengthTwoCompressed
          case object DeadlineOneCompressed extends RangeKeyPartiallyCompressedEntryId(1437) with Deadline.OneCompressed with ValueLengthTwoCompressed
          case object DeadlineTwoCompressed extends RangeKeyPartiallyCompressedEntryId(1438) with Deadline.TwoCompressed with ValueLengthTwoCompressed
          case object DeadlineThreeCompressed extends RangeKeyPartiallyCompressedEntryId(1439) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
          case object DeadlineFourCompressed extends RangeKeyPartiallyCompressedEntryId(1440) with Deadline.FourCompressed with ValueLengthTwoCompressed
          case object DeadlineFiveCompressed extends RangeKeyPartiallyCompressedEntryId(1441) with Deadline.FiveCompressed with ValueLengthTwoCompressed
          case object DeadlineSixCompressed extends RangeKeyPartiallyCompressedEntryId(1442) with Deadline.SixCompressed with ValueLengthTwoCompressed
          case object DeadlineSevenCompressed extends RangeKeyPartiallyCompressedEntryId(1443) with Deadline.SevenCompressed with ValueLengthTwoCompressed
          case object DeadlineFullyCompressed extends RangeKeyPartiallyCompressedEntryId(1444) with Deadline.FullyCompressed with ValueLengthTwoCompressed
          case object DeadlineUncompressed extends RangeKeyPartiallyCompressedEntryId(1445) with Deadline.Uncompressed with ValueLengthTwoCompressed
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
          case object NoDeadline extends RangeKeyPartiallyCompressedEntryId(1446) with Deadline.NoDeadline with ValueLengthThreeCompressed
          case object DeadlineOneCompressed extends RangeKeyPartiallyCompressedEntryId(1447) with Deadline.OneCompressed with ValueLengthThreeCompressed
          case object DeadlineTwoCompressed extends RangeKeyPartiallyCompressedEntryId(1448) with Deadline.TwoCompressed with ValueLengthThreeCompressed
          case object DeadlineThreeCompressed extends RangeKeyPartiallyCompressedEntryId(1449) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
          case object DeadlineFourCompressed extends RangeKeyPartiallyCompressedEntryId(1450) with Deadline.FourCompressed with ValueLengthThreeCompressed
          case object DeadlineFiveCompressed extends RangeKeyPartiallyCompressedEntryId(1451) with Deadline.FiveCompressed with ValueLengthThreeCompressed
          case object DeadlineSixCompressed extends RangeKeyPartiallyCompressedEntryId(1452) with Deadline.SixCompressed with ValueLengthThreeCompressed
          case object DeadlineSevenCompressed extends RangeKeyPartiallyCompressedEntryId(1453) with Deadline.SevenCompressed with ValueLengthThreeCompressed
          case object DeadlineFullyCompressed extends RangeKeyPartiallyCompressedEntryId(1454) with Deadline.FullyCompressed with ValueLengthThreeCompressed
          case object DeadlineUncompressed extends RangeKeyPartiallyCompressedEntryId(1455) with Deadline.Uncompressed with ValueLengthThreeCompressed
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
          case object NoDeadline extends RangeKeyPartiallyCompressedEntryId(1456) with Deadline.NoDeadline with ValueLengthFullyCompressed
          case object DeadlineOneCompressed extends RangeKeyPartiallyCompressedEntryId(1457) with Deadline.OneCompressed with ValueLengthFullyCompressed
          case object DeadlineTwoCompressed extends RangeKeyPartiallyCompressedEntryId(1458) with Deadline.TwoCompressed with ValueLengthFullyCompressed
          case object DeadlineThreeCompressed extends RangeKeyPartiallyCompressedEntryId(1459) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
          case object DeadlineFourCompressed extends RangeKeyPartiallyCompressedEntryId(1460) with Deadline.FourCompressed with ValueLengthFullyCompressed
          case object DeadlineFiveCompressed extends RangeKeyPartiallyCompressedEntryId(1461) with Deadline.FiveCompressed with ValueLengthFullyCompressed
          case object DeadlineSixCompressed extends RangeKeyPartiallyCompressedEntryId(1462) with Deadline.SixCompressed with ValueLengthFullyCompressed
          case object DeadlineSevenCompressed extends RangeKeyPartiallyCompressedEntryId(1463) with Deadline.SevenCompressed with ValueLengthFullyCompressed
          case object DeadlineFullyCompressed extends RangeKeyPartiallyCompressedEntryId(1464) with Deadline.FullyCompressed with ValueLengthFullyCompressed
          case object DeadlineUncompressed extends RangeKeyPartiallyCompressedEntryId(1465) with Deadline.Uncompressed with ValueLengthFullyCompressed
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
          case object NoDeadline extends RangeKeyPartiallyCompressedEntryId(1466) with Deadline.NoDeadline with ValueLengthUncompressed
          case object DeadlineOneCompressed extends RangeKeyPartiallyCompressedEntryId(1467) with Deadline.OneCompressed with ValueLengthUncompressed
          case object DeadlineTwoCompressed extends RangeKeyPartiallyCompressedEntryId(1468) with Deadline.TwoCompressed with ValueLengthUncompressed
          case object DeadlineThreeCompressed extends RangeKeyPartiallyCompressedEntryId(1469) with Deadline.ThreeCompressed with ValueLengthUncompressed
          case object DeadlineFourCompressed extends RangeKeyPartiallyCompressedEntryId(1470) with Deadline.FourCompressed with ValueLengthUncompressed
          case object DeadlineFiveCompressed extends RangeKeyPartiallyCompressedEntryId(1471) with Deadline.FiveCompressed with ValueLengthUncompressed
          case object DeadlineSixCompressed extends RangeKeyPartiallyCompressedEntryId(1472) with Deadline.SixCompressed with ValueLengthUncompressed
          case object DeadlineSevenCompressed extends RangeKeyPartiallyCompressedEntryId(1473) with Deadline.SevenCompressed with ValueLengthUncompressed
          case object DeadlineFullyCompressed extends RangeKeyPartiallyCompressedEntryId(1474) with Deadline.FullyCompressed with ValueLengthUncompressed
          case object DeadlineUncompressed extends RangeKeyPartiallyCompressedEntryId(1475) with Deadline.Uncompressed with ValueLengthUncompressed
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
          case object NoDeadline extends RangeKeyPartiallyCompressedEntryId(1476) with Deadline.NoDeadline with ValueLengthOneCompressed
          case object DeadlineOneCompressed extends RangeKeyPartiallyCompressedEntryId(1477) with Deadline.OneCompressed with ValueLengthOneCompressed
          case object DeadlineTwoCompressed extends RangeKeyPartiallyCompressedEntryId(1478) with Deadline.TwoCompressed with ValueLengthOneCompressed
          case object DeadlineThreeCompressed extends RangeKeyPartiallyCompressedEntryId(1479) with Deadline.ThreeCompressed with ValueLengthOneCompressed
          case object DeadlineFourCompressed extends RangeKeyPartiallyCompressedEntryId(1480) with Deadline.FourCompressed with ValueLengthOneCompressed
          case object DeadlineFiveCompressed extends RangeKeyPartiallyCompressedEntryId(1481) with Deadline.FiveCompressed with ValueLengthOneCompressed
          case object DeadlineSixCompressed extends RangeKeyPartiallyCompressedEntryId(1482) with Deadline.SixCompressed with ValueLengthOneCompressed
          case object DeadlineSevenCompressed extends RangeKeyPartiallyCompressedEntryId(1483) with Deadline.SevenCompressed with ValueLengthOneCompressed
          case object DeadlineFullyCompressed extends RangeKeyPartiallyCompressedEntryId(1484) with Deadline.FullyCompressed with ValueLengthOneCompressed
          case object DeadlineUncompressed extends RangeKeyPartiallyCompressedEntryId(1485) with Deadline.Uncompressed with ValueLengthOneCompressed
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
          case object NoDeadline extends RangeKeyPartiallyCompressedEntryId(1486) with Deadline.NoDeadline with ValueLengthTwoCompressed
          case object DeadlineOneCompressed extends RangeKeyPartiallyCompressedEntryId(1487) with Deadline.OneCompressed with ValueLengthTwoCompressed
          case object DeadlineTwoCompressed extends RangeKeyPartiallyCompressedEntryId(1488) with Deadline.TwoCompressed with ValueLengthTwoCompressed
          case object DeadlineThreeCompressed extends RangeKeyPartiallyCompressedEntryId(1489) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
          case object DeadlineFourCompressed extends RangeKeyPartiallyCompressedEntryId(1490) with Deadline.FourCompressed with ValueLengthTwoCompressed
          case object DeadlineFiveCompressed extends RangeKeyPartiallyCompressedEntryId(1491) with Deadline.FiveCompressed with ValueLengthTwoCompressed
          case object DeadlineSixCompressed extends RangeKeyPartiallyCompressedEntryId(1492) with Deadline.SixCompressed with ValueLengthTwoCompressed
          case object DeadlineSevenCompressed extends RangeKeyPartiallyCompressedEntryId(1493) with Deadline.SevenCompressed with ValueLengthTwoCompressed
          case object DeadlineFullyCompressed extends RangeKeyPartiallyCompressedEntryId(1494) with Deadline.FullyCompressed with ValueLengthTwoCompressed
          case object DeadlineUncompressed extends RangeKeyPartiallyCompressedEntryId(1495) with Deadline.Uncompressed with ValueLengthTwoCompressed
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
          case object NoDeadline extends RangeKeyPartiallyCompressedEntryId(1496) with Deadline.NoDeadline with ValueLengthThreeCompressed
          case object DeadlineOneCompressed extends RangeKeyPartiallyCompressedEntryId(1497) with Deadline.OneCompressed with ValueLengthThreeCompressed
          case object DeadlineTwoCompressed extends RangeKeyPartiallyCompressedEntryId(1498) with Deadline.TwoCompressed with ValueLengthThreeCompressed
          case object DeadlineThreeCompressed extends RangeKeyPartiallyCompressedEntryId(1499) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
          case object DeadlineFourCompressed extends RangeKeyPartiallyCompressedEntryId(1500) with Deadline.FourCompressed with ValueLengthThreeCompressed
          case object DeadlineFiveCompressed extends RangeKeyPartiallyCompressedEntryId(1501) with Deadline.FiveCompressed with ValueLengthThreeCompressed
          case object DeadlineSixCompressed extends RangeKeyPartiallyCompressedEntryId(1502) with Deadline.SixCompressed with ValueLengthThreeCompressed
          case object DeadlineSevenCompressed extends RangeKeyPartiallyCompressedEntryId(1503) with Deadline.SevenCompressed with ValueLengthThreeCompressed
          case object DeadlineFullyCompressed extends RangeKeyPartiallyCompressedEntryId(1504) with Deadline.FullyCompressed with ValueLengthThreeCompressed
          case object DeadlineUncompressed extends RangeKeyPartiallyCompressedEntryId(1505) with Deadline.Uncompressed with ValueLengthThreeCompressed
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
          case object NoDeadline extends RangeKeyPartiallyCompressedEntryId(1506) with Deadline.NoDeadline with ValueLengthFullyCompressed
          case object DeadlineOneCompressed extends RangeKeyPartiallyCompressedEntryId(1507) with Deadline.OneCompressed with ValueLengthFullyCompressed
          case object DeadlineTwoCompressed extends RangeKeyPartiallyCompressedEntryId(1508) with Deadline.TwoCompressed with ValueLengthFullyCompressed
          case object DeadlineThreeCompressed extends RangeKeyPartiallyCompressedEntryId(1509) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
          case object DeadlineFourCompressed extends RangeKeyPartiallyCompressedEntryId(1510) with Deadline.FourCompressed with ValueLengthFullyCompressed
          case object DeadlineFiveCompressed extends RangeKeyPartiallyCompressedEntryId(1511) with Deadline.FiveCompressed with ValueLengthFullyCompressed
          case object DeadlineSixCompressed extends RangeKeyPartiallyCompressedEntryId(1512) with Deadline.SixCompressed with ValueLengthFullyCompressed
          case object DeadlineSevenCompressed extends RangeKeyPartiallyCompressedEntryId(1513) with Deadline.SevenCompressed with ValueLengthFullyCompressed
          case object DeadlineFullyCompressed extends RangeKeyPartiallyCompressedEntryId(1514) with Deadline.FullyCompressed with ValueLengthFullyCompressed
          case object DeadlineUncompressed extends RangeKeyPartiallyCompressedEntryId(1515) with Deadline.Uncompressed with ValueLengthFullyCompressed
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
          case object NoDeadline extends RangeKeyPartiallyCompressedEntryId(1516) with Deadline.NoDeadline with ValueLengthUncompressed
          case object DeadlineOneCompressed extends RangeKeyPartiallyCompressedEntryId(1517) with Deadline.OneCompressed with ValueLengthUncompressed
          case object DeadlineTwoCompressed extends RangeKeyPartiallyCompressedEntryId(1518) with Deadline.TwoCompressed with ValueLengthUncompressed
          case object DeadlineThreeCompressed extends RangeKeyPartiallyCompressedEntryId(1519) with Deadline.ThreeCompressed with ValueLengthUncompressed
          case object DeadlineFourCompressed extends RangeKeyPartiallyCompressedEntryId(1520) with Deadline.FourCompressed with ValueLengthUncompressed
          case object DeadlineFiveCompressed extends RangeKeyPartiallyCompressedEntryId(1521) with Deadline.FiveCompressed with ValueLengthUncompressed
          case object DeadlineSixCompressed extends RangeKeyPartiallyCompressedEntryId(1522) with Deadline.SixCompressed with ValueLengthUncompressed
          case object DeadlineSevenCompressed extends RangeKeyPartiallyCompressedEntryId(1523) with Deadline.SevenCompressed with ValueLengthUncompressed
          case object DeadlineFullyCompressed extends RangeKeyPartiallyCompressedEntryId(1524) with Deadline.FullyCompressed with ValueLengthUncompressed
          case object DeadlineUncompressed extends RangeKeyPartiallyCompressedEntryId(1525) with Deadline.Uncompressed with ValueLengthUncompressed
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
          case object NoDeadline extends RangeKeyPartiallyCompressedEntryId(1526) with Deadline.NoDeadline with ValueLengthOneCompressed
          case object DeadlineOneCompressed extends RangeKeyPartiallyCompressedEntryId(1527) with Deadline.OneCompressed with ValueLengthOneCompressed
          case object DeadlineTwoCompressed extends RangeKeyPartiallyCompressedEntryId(1528) with Deadline.TwoCompressed with ValueLengthOneCompressed
          case object DeadlineThreeCompressed extends RangeKeyPartiallyCompressedEntryId(1529) with Deadline.ThreeCompressed with ValueLengthOneCompressed
          case object DeadlineFourCompressed extends RangeKeyPartiallyCompressedEntryId(1530) with Deadline.FourCompressed with ValueLengthOneCompressed
          case object DeadlineFiveCompressed extends RangeKeyPartiallyCompressedEntryId(1531) with Deadline.FiveCompressed with ValueLengthOneCompressed
          case object DeadlineSixCompressed extends RangeKeyPartiallyCompressedEntryId(1532) with Deadline.SixCompressed with ValueLengthOneCompressed
          case object DeadlineSevenCompressed extends RangeKeyPartiallyCompressedEntryId(1533) with Deadline.SevenCompressed with ValueLengthOneCompressed
          case object DeadlineFullyCompressed extends RangeKeyPartiallyCompressedEntryId(1534) with Deadline.FullyCompressed with ValueLengthOneCompressed
          case object DeadlineUncompressed extends RangeKeyPartiallyCompressedEntryId(1535) with Deadline.Uncompressed with ValueLengthOneCompressed
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
          case object NoDeadline extends RangeKeyPartiallyCompressedEntryId(1536) with Deadline.NoDeadline with ValueLengthTwoCompressed
          case object DeadlineOneCompressed extends RangeKeyPartiallyCompressedEntryId(1537) with Deadline.OneCompressed with ValueLengthTwoCompressed
          case object DeadlineTwoCompressed extends RangeKeyPartiallyCompressedEntryId(1538) with Deadline.TwoCompressed with ValueLengthTwoCompressed
          case object DeadlineThreeCompressed extends RangeKeyPartiallyCompressedEntryId(1539) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
          case object DeadlineFourCompressed extends RangeKeyPartiallyCompressedEntryId(1540) with Deadline.FourCompressed with ValueLengthTwoCompressed
          case object DeadlineFiveCompressed extends RangeKeyPartiallyCompressedEntryId(1541) with Deadline.FiveCompressed with ValueLengthTwoCompressed
          case object DeadlineSixCompressed extends RangeKeyPartiallyCompressedEntryId(1542) with Deadline.SixCompressed with ValueLengthTwoCompressed
          case object DeadlineSevenCompressed extends RangeKeyPartiallyCompressedEntryId(1543) with Deadline.SevenCompressed with ValueLengthTwoCompressed
          case object DeadlineFullyCompressed extends RangeKeyPartiallyCompressedEntryId(1544) with Deadline.FullyCompressed with ValueLengthTwoCompressed
          case object DeadlineUncompressed extends RangeKeyPartiallyCompressedEntryId(1545) with Deadline.Uncompressed with ValueLengthTwoCompressed
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
          case object NoDeadline extends RangeKeyPartiallyCompressedEntryId(1546) with Deadline.NoDeadline with ValueLengthThreeCompressed
          case object DeadlineOneCompressed extends RangeKeyPartiallyCompressedEntryId(1547) with Deadline.OneCompressed with ValueLengthThreeCompressed
          case object DeadlineTwoCompressed extends RangeKeyPartiallyCompressedEntryId(1548) with Deadline.TwoCompressed with ValueLengthThreeCompressed
          case object DeadlineThreeCompressed extends RangeKeyPartiallyCompressedEntryId(1549) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
          case object DeadlineFourCompressed extends RangeKeyPartiallyCompressedEntryId(1550) with Deadline.FourCompressed with ValueLengthThreeCompressed
          case object DeadlineFiveCompressed extends RangeKeyPartiallyCompressedEntryId(1551) with Deadline.FiveCompressed with ValueLengthThreeCompressed
          case object DeadlineSixCompressed extends RangeKeyPartiallyCompressedEntryId(1552) with Deadline.SixCompressed with ValueLengthThreeCompressed
          case object DeadlineSevenCompressed extends RangeKeyPartiallyCompressedEntryId(1553) with Deadline.SevenCompressed with ValueLengthThreeCompressed
          case object DeadlineFullyCompressed extends RangeKeyPartiallyCompressedEntryId(1554) with Deadline.FullyCompressed with ValueLengthThreeCompressed
          case object DeadlineUncompressed extends RangeKeyPartiallyCompressedEntryId(1555) with Deadline.Uncompressed with ValueLengthThreeCompressed
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
          case object NoDeadline extends RangeKeyPartiallyCompressedEntryId(1556) with Deadline.NoDeadline with ValueLengthFullyCompressed
          case object DeadlineOneCompressed extends RangeKeyPartiallyCompressedEntryId(1557) with Deadline.OneCompressed with ValueLengthFullyCompressed
          case object DeadlineTwoCompressed extends RangeKeyPartiallyCompressedEntryId(1558) with Deadline.TwoCompressed with ValueLengthFullyCompressed
          case object DeadlineThreeCompressed extends RangeKeyPartiallyCompressedEntryId(1559) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
          case object DeadlineFourCompressed extends RangeKeyPartiallyCompressedEntryId(1560) with Deadline.FourCompressed with ValueLengthFullyCompressed
          case object DeadlineFiveCompressed extends RangeKeyPartiallyCompressedEntryId(1561) with Deadline.FiveCompressed with ValueLengthFullyCompressed
          case object DeadlineSixCompressed extends RangeKeyPartiallyCompressedEntryId(1562) with Deadline.SixCompressed with ValueLengthFullyCompressed
          case object DeadlineSevenCompressed extends RangeKeyPartiallyCompressedEntryId(1563) with Deadline.SevenCompressed with ValueLengthFullyCompressed
          case object DeadlineFullyCompressed extends RangeKeyPartiallyCompressedEntryId(1564) with Deadline.FullyCompressed with ValueLengthFullyCompressed
          case object DeadlineUncompressed extends RangeKeyPartiallyCompressedEntryId(1565) with Deadline.Uncompressed with ValueLengthFullyCompressed
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
          case object NoDeadline extends RangeKeyPartiallyCompressedEntryId(1566) with Deadline.NoDeadline with ValueLengthUncompressed
          case object DeadlineOneCompressed extends RangeKeyPartiallyCompressedEntryId(1567) with Deadline.OneCompressed with ValueLengthUncompressed
          case object DeadlineTwoCompressed extends RangeKeyPartiallyCompressedEntryId(1568) with Deadline.TwoCompressed with ValueLengthUncompressed
          case object DeadlineThreeCompressed extends RangeKeyPartiallyCompressedEntryId(1569) with Deadline.ThreeCompressed with ValueLengthUncompressed
          case object DeadlineFourCompressed extends RangeKeyPartiallyCompressedEntryId(1570) with Deadline.FourCompressed with ValueLengthUncompressed
          case object DeadlineFiveCompressed extends RangeKeyPartiallyCompressedEntryId(1571) with Deadline.FiveCompressed with ValueLengthUncompressed
          case object DeadlineSixCompressed extends RangeKeyPartiallyCompressedEntryId(1572) with Deadline.SixCompressed with ValueLengthUncompressed
          case object DeadlineSevenCompressed extends RangeKeyPartiallyCompressedEntryId(1573) with Deadline.SevenCompressed with ValueLengthUncompressed
          case object DeadlineFullyCompressed extends RangeKeyPartiallyCompressedEntryId(1574) with Deadline.FullyCompressed with ValueLengthUncompressed
          case object DeadlineUncompressed extends RangeKeyPartiallyCompressedEntryId(1575) with Deadline.Uncompressed with ValueLengthUncompressed
        }
      }
    }
  }

  def keyIdsList: List[RangeKeyPartiallyCompressedEntryId] = SealedList.list[RangeKeyPartiallyCompressedEntryId].sortBy(_.id)

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