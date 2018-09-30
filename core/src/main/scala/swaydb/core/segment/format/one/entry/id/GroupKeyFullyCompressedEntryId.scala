
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
sealed abstract class GroupKeyFullyCompressedEntryId(override val id: Int) extends EntryId(id)
object GroupKeyFullyCompressedEntryId {

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
      case object NoDeadline extends GroupKeyFullyCompressedEntryId(1135) with Deadline.NoDeadline with NoValue
      case object DeadlineOneCompressed extends GroupKeyFullyCompressedEntryId(1136) with Deadline.OneCompressed with NoValue
      case object DeadlineTwoCompressed extends GroupKeyFullyCompressedEntryId(1137) with Deadline.TwoCompressed with NoValue
      case object DeadlineThreeCompressed extends GroupKeyFullyCompressedEntryId(1138) with Deadline.ThreeCompressed with NoValue
      case object DeadlineFourCompressed extends GroupKeyFullyCompressedEntryId(1139) with Deadline.FourCompressed with NoValue
      case object DeadlineFiveCompressed extends GroupKeyFullyCompressedEntryId(1140) with Deadline.FiveCompressed with NoValue
      case object DeadlineSixCompressed extends GroupKeyFullyCompressedEntryId(1141) with Deadline.SixCompressed with NoValue
      case object DeadlineSevenCompressed extends GroupKeyFullyCompressedEntryId(1142) with Deadline.SevenCompressed with NoValue
      case object DeadlineFullyCompressed extends GroupKeyFullyCompressedEntryId(1143) with Deadline.FullyCompressed with NoValue
      case object DeadlineUncompressed extends GroupKeyFullyCompressedEntryId(1144) with Deadline.Uncompressed with NoValue
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
      case object NoDeadline extends GroupKeyFullyCompressedEntryId(1145) with Deadline.NoDeadline with ValueFullyCompressed
      case object DeadlineOneCompressed extends GroupKeyFullyCompressedEntryId(1146) with Deadline.OneCompressed with ValueFullyCompressed
      case object DeadlineTwoCompressed extends GroupKeyFullyCompressedEntryId(1147) with Deadline.TwoCompressed with ValueFullyCompressed
      case object DeadlineThreeCompressed extends GroupKeyFullyCompressedEntryId(1148) with Deadline.ThreeCompressed with ValueFullyCompressed
      case object DeadlineFourCompressed extends GroupKeyFullyCompressedEntryId(1149) with Deadline.FourCompressed with ValueFullyCompressed
      case object DeadlineFiveCompressed extends GroupKeyFullyCompressedEntryId(1150) with Deadline.FiveCompressed with ValueFullyCompressed
      case object DeadlineSixCompressed extends GroupKeyFullyCompressedEntryId(1151) with Deadline.SixCompressed with ValueFullyCompressed
      case object DeadlineSevenCompressed extends GroupKeyFullyCompressedEntryId(1152) with Deadline.SevenCompressed with ValueFullyCompressed
      case object DeadlineFullyCompressed extends GroupKeyFullyCompressedEntryId(1153) with Deadline.FullyCompressed with ValueFullyCompressed
      case object DeadlineUncompressed extends GroupKeyFullyCompressedEntryId(1154) with Deadline.Uncompressed with ValueFullyCompressed
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
          case object NoDeadline extends GroupKeyFullyCompressedEntryId(1155) with Deadline.NoDeadline with ValueLengthOneCompressed
          case object DeadlineOneCompressed extends GroupKeyFullyCompressedEntryId(1156) with Deadline.OneCompressed with ValueLengthOneCompressed
          case object DeadlineTwoCompressed extends GroupKeyFullyCompressedEntryId(1157) with Deadline.TwoCompressed with ValueLengthOneCompressed
          case object DeadlineThreeCompressed extends GroupKeyFullyCompressedEntryId(1158) with Deadline.ThreeCompressed with ValueLengthOneCompressed
          case object DeadlineFourCompressed extends GroupKeyFullyCompressedEntryId(1159) with Deadline.FourCompressed with ValueLengthOneCompressed
          case object DeadlineFiveCompressed extends GroupKeyFullyCompressedEntryId(1160) with Deadline.FiveCompressed with ValueLengthOneCompressed
          case object DeadlineSixCompressed extends GroupKeyFullyCompressedEntryId(1161) with Deadline.SixCompressed with ValueLengthOneCompressed
          case object DeadlineSevenCompressed extends GroupKeyFullyCompressedEntryId(1162) with Deadline.SevenCompressed with ValueLengthOneCompressed
          case object DeadlineFullyCompressed extends GroupKeyFullyCompressedEntryId(1163) with Deadline.FullyCompressed with ValueLengthOneCompressed
          case object DeadlineUncompressed extends GroupKeyFullyCompressedEntryId(1164) with Deadline.Uncompressed with ValueLengthOneCompressed
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
          case object NoDeadline extends GroupKeyFullyCompressedEntryId(1165) with Deadline.NoDeadline with ValueLengthTwoCompressed
          case object DeadlineOneCompressed extends GroupKeyFullyCompressedEntryId(1166) with Deadline.OneCompressed with ValueLengthTwoCompressed
          case object DeadlineTwoCompressed extends GroupKeyFullyCompressedEntryId(1167) with Deadline.TwoCompressed with ValueLengthTwoCompressed
          case object DeadlineThreeCompressed extends GroupKeyFullyCompressedEntryId(1168) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
          case object DeadlineFourCompressed extends GroupKeyFullyCompressedEntryId(1169) with Deadline.FourCompressed with ValueLengthTwoCompressed
          case object DeadlineFiveCompressed extends GroupKeyFullyCompressedEntryId(1170) with Deadline.FiveCompressed with ValueLengthTwoCompressed
          case object DeadlineSixCompressed extends GroupKeyFullyCompressedEntryId(1171) with Deadline.SixCompressed with ValueLengthTwoCompressed
          case object DeadlineSevenCompressed extends GroupKeyFullyCompressedEntryId(1172) with Deadline.SevenCompressed with ValueLengthTwoCompressed
          case object DeadlineFullyCompressed extends GroupKeyFullyCompressedEntryId(1173) with Deadline.FullyCompressed with ValueLengthTwoCompressed
          case object DeadlineUncompressed extends GroupKeyFullyCompressedEntryId(1174) with Deadline.Uncompressed with ValueLengthTwoCompressed
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
          case object NoDeadline extends GroupKeyFullyCompressedEntryId(1175) with Deadline.NoDeadline with ValueLengthThreeCompressed
          case object DeadlineOneCompressed extends GroupKeyFullyCompressedEntryId(1176) with Deadline.OneCompressed with ValueLengthThreeCompressed
          case object DeadlineTwoCompressed extends GroupKeyFullyCompressedEntryId(1177) with Deadline.TwoCompressed with ValueLengthThreeCompressed
          case object DeadlineThreeCompressed extends GroupKeyFullyCompressedEntryId(1178) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
          case object DeadlineFourCompressed extends GroupKeyFullyCompressedEntryId(1179) with Deadline.FourCompressed with ValueLengthThreeCompressed
          case object DeadlineFiveCompressed extends GroupKeyFullyCompressedEntryId(1180) with Deadline.FiveCompressed with ValueLengthThreeCompressed
          case object DeadlineSixCompressed extends GroupKeyFullyCompressedEntryId(1181) with Deadline.SixCompressed with ValueLengthThreeCompressed
          case object DeadlineSevenCompressed extends GroupKeyFullyCompressedEntryId(1182) with Deadline.SevenCompressed with ValueLengthThreeCompressed
          case object DeadlineFullyCompressed extends GroupKeyFullyCompressedEntryId(1183) with Deadline.FullyCompressed with ValueLengthThreeCompressed
          case object DeadlineUncompressed extends GroupKeyFullyCompressedEntryId(1184) with Deadline.Uncompressed with ValueLengthThreeCompressed
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
          case object NoDeadline extends GroupKeyFullyCompressedEntryId(1185) with Deadline.NoDeadline with ValueLengthFullyCompressed
          case object DeadlineOneCompressed extends GroupKeyFullyCompressedEntryId(1186) with Deadline.OneCompressed with ValueLengthFullyCompressed
          case object DeadlineTwoCompressed extends GroupKeyFullyCompressedEntryId(1187) with Deadline.TwoCompressed with ValueLengthFullyCompressed
          case object DeadlineThreeCompressed extends GroupKeyFullyCompressedEntryId(1188) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
          case object DeadlineFourCompressed extends GroupKeyFullyCompressedEntryId(1189) with Deadline.FourCompressed with ValueLengthFullyCompressed
          case object DeadlineFiveCompressed extends GroupKeyFullyCompressedEntryId(1190) with Deadline.FiveCompressed with ValueLengthFullyCompressed
          case object DeadlineSixCompressed extends GroupKeyFullyCompressedEntryId(1191) with Deadline.SixCompressed with ValueLengthFullyCompressed
          case object DeadlineSevenCompressed extends GroupKeyFullyCompressedEntryId(1192) with Deadline.SevenCompressed with ValueLengthFullyCompressed
          case object DeadlineFullyCompressed extends GroupKeyFullyCompressedEntryId(1193) with Deadline.FullyCompressed with ValueLengthFullyCompressed
          case object DeadlineUncompressed extends GroupKeyFullyCompressedEntryId(1194) with Deadline.Uncompressed with ValueLengthFullyCompressed
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
          case object NoDeadline extends GroupKeyFullyCompressedEntryId(1195) with Deadline.NoDeadline with ValueLengthUncompressed
          case object DeadlineOneCompressed extends GroupKeyFullyCompressedEntryId(1196) with Deadline.OneCompressed with ValueLengthUncompressed
          case object DeadlineTwoCompressed extends GroupKeyFullyCompressedEntryId(1197) with Deadline.TwoCompressed with ValueLengthUncompressed
          case object DeadlineThreeCompressed extends GroupKeyFullyCompressedEntryId(1198) with Deadline.ThreeCompressed with ValueLengthUncompressed
          case object DeadlineFourCompressed extends GroupKeyFullyCompressedEntryId(1199) with Deadline.FourCompressed with ValueLengthUncompressed
          case object DeadlineFiveCompressed extends GroupKeyFullyCompressedEntryId(1200) with Deadline.FiveCompressed with ValueLengthUncompressed
          case object DeadlineSixCompressed extends GroupKeyFullyCompressedEntryId(1201) with Deadline.SixCompressed with ValueLengthUncompressed
          case object DeadlineSevenCompressed extends GroupKeyFullyCompressedEntryId(1202) with Deadline.SevenCompressed with ValueLengthUncompressed
          case object DeadlineFullyCompressed extends GroupKeyFullyCompressedEntryId(1203) with Deadline.FullyCompressed with ValueLengthUncompressed
          case object DeadlineUncompressed extends GroupKeyFullyCompressedEntryId(1204) with Deadline.Uncompressed with ValueLengthUncompressed
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
          case object NoDeadline extends GroupKeyFullyCompressedEntryId(1205) with Deadline.NoDeadline with ValueLengthOneCompressed
          case object DeadlineOneCompressed extends GroupKeyFullyCompressedEntryId(1206) with Deadline.OneCompressed with ValueLengthOneCompressed
          case object DeadlineTwoCompressed extends GroupKeyFullyCompressedEntryId(1207) with Deadline.TwoCompressed with ValueLengthOneCompressed
          case object DeadlineThreeCompressed extends GroupKeyFullyCompressedEntryId(1208) with Deadline.ThreeCompressed with ValueLengthOneCompressed
          case object DeadlineFourCompressed extends GroupKeyFullyCompressedEntryId(1209) with Deadline.FourCompressed with ValueLengthOneCompressed
          case object DeadlineFiveCompressed extends GroupKeyFullyCompressedEntryId(1210) with Deadline.FiveCompressed with ValueLengthOneCompressed
          case object DeadlineSixCompressed extends GroupKeyFullyCompressedEntryId(1211) with Deadline.SixCompressed with ValueLengthOneCompressed
          case object DeadlineSevenCompressed extends GroupKeyFullyCompressedEntryId(1212) with Deadline.SevenCompressed with ValueLengthOneCompressed
          case object DeadlineFullyCompressed extends GroupKeyFullyCompressedEntryId(1213) with Deadline.FullyCompressed with ValueLengthOneCompressed
          case object DeadlineUncompressed extends GroupKeyFullyCompressedEntryId(1214) with Deadline.Uncompressed with ValueLengthOneCompressed
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
          case object NoDeadline extends GroupKeyFullyCompressedEntryId(1215) with Deadline.NoDeadline with ValueLengthTwoCompressed
          case object DeadlineOneCompressed extends GroupKeyFullyCompressedEntryId(1216) with Deadline.OneCompressed with ValueLengthTwoCompressed
          case object DeadlineTwoCompressed extends GroupKeyFullyCompressedEntryId(1217) with Deadline.TwoCompressed with ValueLengthTwoCompressed
          case object DeadlineThreeCompressed extends GroupKeyFullyCompressedEntryId(1218) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
          case object DeadlineFourCompressed extends GroupKeyFullyCompressedEntryId(1219) with Deadline.FourCompressed with ValueLengthTwoCompressed
          case object DeadlineFiveCompressed extends GroupKeyFullyCompressedEntryId(1220) with Deadline.FiveCompressed with ValueLengthTwoCompressed
          case object DeadlineSixCompressed extends GroupKeyFullyCompressedEntryId(1221) with Deadline.SixCompressed with ValueLengthTwoCompressed
          case object DeadlineSevenCompressed extends GroupKeyFullyCompressedEntryId(1222) with Deadline.SevenCompressed with ValueLengthTwoCompressed
          case object DeadlineFullyCompressed extends GroupKeyFullyCompressedEntryId(1223) with Deadline.FullyCompressed with ValueLengthTwoCompressed
          case object DeadlineUncompressed extends GroupKeyFullyCompressedEntryId(1224) with Deadline.Uncompressed with ValueLengthTwoCompressed
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
          case object NoDeadline extends GroupKeyFullyCompressedEntryId(1225) with Deadline.NoDeadline with ValueLengthThreeCompressed
          case object DeadlineOneCompressed extends GroupKeyFullyCompressedEntryId(1226) with Deadline.OneCompressed with ValueLengthThreeCompressed
          case object DeadlineTwoCompressed extends GroupKeyFullyCompressedEntryId(1227) with Deadline.TwoCompressed with ValueLengthThreeCompressed
          case object DeadlineThreeCompressed extends GroupKeyFullyCompressedEntryId(1228) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
          case object DeadlineFourCompressed extends GroupKeyFullyCompressedEntryId(1229) with Deadline.FourCompressed with ValueLengthThreeCompressed
          case object DeadlineFiveCompressed extends GroupKeyFullyCompressedEntryId(1230) with Deadline.FiveCompressed with ValueLengthThreeCompressed
          case object DeadlineSixCompressed extends GroupKeyFullyCompressedEntryId(1231) with Deadline.SixCompressed with ValueLengthThreeCompressed
          case object DeadlineSevenCompressed extends GroupKeyFullyCompressedEntryId(1232) with Deadline.SevenCompressed with ValueLengthThreeCompressed
          case object DeadlineFullyCompressed extends GroupKeyFullyCompressedEntryId(1233) with Deadline.FullyCompressed with ValueLengthThreeCompressed
          case object DeadlineUncompressed extends GroupKeyFullyCompressedEntryId(1234) with Deadline.Uncompressed with ValueLengthThreeCompressed
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
          case object NoDeadline extends GroupKeyFullyCompressedEntryId(1235) with Deadline.NoDeadline with ValueLengthFullyCompressed
          case object DeadlineOneCompressed extends GroupKeyFullyCompressedEntryId(1236) with Deadline.OneCompressed with ValueLengthFullyCompressed
          case object DeadlineTwoCompressed extends GroupKeyFullyCompressedEntryId(1237) with Deadline.TwoCompressed with ValueLengthFullyCompressed
          case object DeadlineThreeCompressed extends GroupKeyFullyCompressedEntryId(1238) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
          case object DeadlineFourCompressed extends GroupKeyFullyCompressedEntryId(1239) with Deadline.FourCompressed with ValueLengthFullyCompressed
          case object DeadlineFiveCompressed extends GroupKeyFullyCompressedEntryId(1240) with Deadline.FiveCompressed with ValueLengthFullyCompressed
          case object DeadlineSixCompressed extends GroupKeyFullyCompressedEntryId(1241) with Deadline.SixCompressed with ValueLengthFullyCompressed
          case object DeadlineSevenCompressed extends GroupKeyFullyCompressedEntryId(1242) with Deadline.SevenCompressed with ValueLengthFullyCompressed
          case object DeadlineFullyCompressed extends GroupKeyFullyCompressedEntryId(1243) with Deadline.FullyCompressed with ValueLengthFullyCompressed
          case object DeadlineUncompressed extends GroupKeyFullyCompressedEntryId(1244) with Deadline.Uncompressed with ValueLengthFullyCompressed
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
          case object NoDeadline extends GroupKeyFullyCompressedEntryId(1245) with Deadline.NoDeadline with ValueLengthUncompressed
          case object DeadlineOneCompressed extends GroupKeyFullyCompressedEntryId(1246) with Deadline.OneCompressed with ValueLengthUncompressed
          case object DeadlineTwoCompressed extends GroupKeyFullyCompressedEntryId(1247) with Deadline.TwoCompressed with ValueLengthUncompressed
          case object DeadlineThreeCompressed extends GroupKeyFullyCompressedEntryId(1248) with Deadline.ThreeCompressed with ValueLengthUncompressed
          case object DeadlineFourCompressed extends GroupKeyFullyCompressedEntryId(1249) with Deadline.FourCompressed with ValueLengthUncompressed
          case object DeadlineFiveCompressed extends GroupKeyFullyCompressedEntryId(1250) with Deadline.FiveCompressed with ValueLengthUncompressed
          case object DeadlineSixCompressed extends GroupKeyFullyCompressedEntryId(1251) with Deadline.SixCompressed with ValueLengthUncompressed
          case object DeadlineSevenCompressed extends GroupKeyFullyCompressedEntryId(1252) with Deadline.SevenCompressed with ValueLengthUncompressed
          case object DeadlineFullyCompressed extends GroupKeyFullyCompressedEntryId(1253) with Deadline.FullyCompressed with ValueLengthUncompressed
          case object DeadlineUncompressed extends GroupKeyFullyCompressedEntryId(1254) with Deadline.Uncompressed with ValueLengthUncompressed
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
          case object NoDeadline extends GroupKeyFullyCompressedEntryId(1255) with Deadline.NoDeadline with ValueLengthOneCompressed
          case object DeadlineOneCompressed extends GroupKeyFullyCompressedEntryId(1256) with Deadline.OneCompressed with ValueLengthOneCompressed
          case object DeadlineTwoCompressed extends GroupKeyFullyCompressedEntryId(1257) with Deadline.TwoCompressed with ValueLengthOneCompressed
          case object DeadlineThreeCompressed extends GroupKeyFullyCompressedEntryId(1258) with Deadline.ThreeCompressed with ValueLengthOneCompressed
          case object DeadlineFourCompressed extends GroupKeyFullyCompressedEntryId(1259) with Deadline.FourCompressed with ValueLengthOneCompressed
          case object DeadlineFiveCompressed extends GroupKeyFullyCompressedEntryId(1260) with Deadline.FiveCompressed with ValueLengthOneCompressed
          case object DeadlineSixCompressed extends GroupKeyFullyCompressedEntryId(1261) with Deadline.SixCompressed with ValueLengthOneCompressed
          case object DeadlineSevenCompressed extends GroupKeyFullyCompressedEntryId(1262) with Deadline.SevenCompressed with ValueLengthOneCompressed
          case object DeadlineFullyCompressed extends GroupKeyFullyCompressedEntryId(1263) with Deadline.FullyCompressed with ValueLengthOneCompressed
          case object DeadlineUncompressed extends GroupKeyFullyCompressedEntryId(1264) with Deadline.Uncompressed with ValueLengthOneCompressed
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
          case object NoDeadline extends GroupKeyFullyCompressedEntryId(1265) with Deadline.NoDeadline with ValueLengthTwoCompressed
          case object DeadlineOneCompressed extends GroupKeyFullyCompressedEntryId(1266) with Deadline.OneCompressed with ValueLengthTwoCompressed
          case object DeadlineTwoCompressed extends GroupKeyFullyCompressedEntryId(1267) with Deadline.TwoCompressed with ValueLengthTwoCompressed
          case object DeadlineThreeCompressed extends GroupKeyFullyCompressedEntryId(1268) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
          case object DeadlineFourCompressed extends GroupKeyFullyCompressedEntryId(1269) with Deadline.FourCompressed with ValueLengthTwoCompressed
          case object DeadlineFiveCompressed extends GroupKeyFullyCompressedEntryId(1270) with Deadline.FiveCompressed with ValueLengthTwoCompressed
          case object DeadlineSixCompressed extends GroupKeyFullyCompressedEntryId(1271) with Deadline.SixCompressed with ValueLengthTwoCompressed
          case object DeadlineSevenCompressed extends GroupKeyFullyCompressedEntryId(1272) with Deadline.SevenCompressed with ValueLengthTwoCompressed
          case object DeadlineFullyCompressed extends GroupKeyFullyCompressedEntryId(1273) with Deadline.FullyCompressed with ValueLengthTwoCompressed
          case object DeadlineUncompressed extends GroupKeyFullyCompressedEntryId(1274) with Deadline.Uncompressed with ValueLengthTwoCompressed
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
          case object NoDeadline extends GroupKeyFullyCompressedEntryId(1275) with Deadline.NoDeadline with ValueLengthThreeCompressed
          case object DeadlineOneCompressed extends GroupKeyFullyCompressedEntryId(1276) with Deadline.OneCompressed with ValueLengthThreeCompressed
          case object DeadlineTwoCompressed extends GroupKeyFullyCompressedEntryId(1277) with Deadline.TwoCompressed with ValueLengthThreeCompressed
          case object DeadlineThreeCompressed extends GroupKeyFullyCompressedEntryId(1278) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
          case object DeadlineFourCompressed extends GroupKeyFullyCompressedEntryId(1279) with Deadline.FourCompressed with ValueLengthThreeCompressed
          case object DeadlineFiveCompressed extends GroupKeyFullyCompressedEntryId(1280) with Deadline.FiveCompressed with ValueLengthThreeCompressed
          case object DeadlineSixCompressed extends GroupKeyFullyCompressedEntryId(1281) with Deadline.SixCompressed with ValueLengthThreeCompressed
          case object DeadlineSevenCompressed extends GroupKeyFullyCompressedEntryId(1282) with Deadline.SevenCompressed with ValueLengthThreeCompressed
          case object DeadlineFullyCompressed extends GroupKeyFullyCompressedEntryId(1283) with Deadline.FullyCompressed with ValueLengthThreeCompressed
          case object DeadlineUncompressed extends GroupKeyFullyCompressedEntryId(1284) with Deadline.Uncompressed with ValueLengthThreeCompressed
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
          case object NoDeadline extends GroupKeyFullyCompressedEntryId(1285) with Deadline.NoDeadline with ValueLengthFullyCompressed
          case object DeadlineOneCompressed extends GroupKeyFullyCompressedEntryId(1286) with Deadline.OneCompressed with ValueLengthFullyCompressed
          case object DeadlineTwoCompressed extends GroupKeyFullyCompressedEntryId(1287) with Deadline.TwoCompressed with ValueLengthFullyCompressed
          case object DeadlineThreeCompressed extends GroupKeyFullyCompressedEntryId(1288) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
          case object DeadlineFourCompressed extends GroupKeyFullyCompressedEntryId(1289) with Deadline.FourCompressed with ValueLengthFullyCompressed
          case object DeadlineFiveCompressed extends GroupKeyFullyCompressedEntryId(1290) with Deadline.FiveCompressed with ValueLengthFullyCompressed
          case object DeadlineSixCompressed extends GroupKeyFullyCompressedEntryId(1291) with Deadline.SixCompressed with ValueLengthFullyCompressed
          case object DeadlineSevenCompressed extends GroupKeyFullyCompressedEntryId(1292) with Deadline.SevenCompressed with ValueLengthFullyCompressed
          case object DeadlineFullyCompressed extends GroupKeyFullyCompressedEntryId(1293) with Deadline.FullyCompressed with ValueLengthFullyCompressed
          case object DeadlineUncompressed extends GroupKeyFullyCompressedEntryId(1294) with Deadline.Uncompressed with ValueLengthFullyCompressed
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
          case object NoDeadline extends GroupKeyFullyCompressedEntryId(1295) with Deadline.NoDeadline with ValueLengthUncompressed
          case object DeadlineOneCompressed extends GroupKeyFullyCompressedEntryId(1296) with Deadline.OneCompressed with ValueLengthUncompressed
          case object DeadlineTwoCompressed extends GroupKeyFullyCompressedEntryId(1297) with Deadline.TwoCompressed with ValueLengthUncompressed
          case object DeadlineThreeCompressed extends GroupKeyFullyCompressedEntryId(1298) with Deadline.ThreeCompressed with ValueLengthUncompressed
          case object DeadlineFourCompressed extends GroupKeyFullyCompressedEntryId(1299) with Deadline.FourCompressed with ValueLengthUncompressed
          case object DeadlineFiveCompressed extends GroupKeyFullyCompressedEntryId(1300) with Deadline.FiveCompressed with ValueLengthUncompressed
          case object DeadlineSixCompressed extends GroupKeyFullyCompressedEntryId(1301) with Deadline.SixCompressed with ValueLengthUncompressed
          case object DeadlineSevenCompressed extends GroupKeyFullyCompressedEntryId(1302) with Deadline.SevenCompressed with ValueLengthUncompressed
          case object DeadlineFullyCompressed extends GroupKeyFullyCompressedEntryId(1303) with Deadline.FullyCompressed with ValueLengthUncompressed
          case object DeadlineUncompressed extends GroupKeyFullyCompressedEntryId(1304) with Deadline.Uncompressed with ValueLengthUncompressed
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
          case object NoDeadline extends GroupKeyFullyCompressedEntryId(1305) with Deadline.NoDeadline with ValueLengthOneCompressed
          case object DeadlineOneCompressed extends GroupKeyFullyCompressedEntryId(1306) with Deadline.OneCompressed with ValueLengthOneCompressed
          case object DeadlineTwoCompressed extends GroupKeyFullyCompressedEntryId(1307) with Deadline.TwoCompressed with ValueLengthOneCompressed
          case object DeadlineThreeCompressed extends GroupKeyFullyCompressedEntryId(1308) with Deadline.ThreeCompressed with ValueLengthOneCompressed
          case object DeadlineFourCompressed extends GroupKeyFullyCompressedEntryId(1309) with Deadline.FourCompressed with ValueLengthOneCompressed
          case object DeadlineFiveCompressed extends GroupKeyFullyCompressedEntryId(1310) with Deadline.FiveCompressed with ValueLengthOneCompressed
          case object DeadlineSixCompressed extends GroupKeyFullyCompressedEntryId(1311) with Deadline.SixCompressed with ValueLengthOneCompressed
          case object DeadlineSevenCompressed extends GroupKeyFullyCompressedEntryId(1312) with Deadline.SevenCompressed with ValueLengthOneCompressed
          case object DeadlineFullyCompressed extends GroupKeyFullyCompressedEntryId(1313) with Deadline.FullyCompressed with ValueLengthOneCompressed
          case object DeadlineUncompressed extends GroupKeyFullyCompressedEntryId(1314) with Deadline.Uncompressed with ValueLengthOneCompressed
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
          case object NoDeadline extends GroupKeyFullyCompressedEntryId(1315) with Deadline.NoDeadline with ValueLengthTwoCompressed
          case object DeadlineOneCompressed extends GroupKeyFullyCompressedEntryId(1316) with Deadline.OneCompressed with ValueLengthTwoCompressed
          case object DeadlineTwoCompressed extends GroupKeyFullyCompressedEntryId(1317) with Deadline.TwoCompressed with ValueLengthTwoCompressed
          case object DeadlineThreeCompressed extends GroupKeyFullyCompressedEntryId(1318) with Deadline.ThreeCompressed with ValueLengthTwoCompressed
          case object DeadlineFourCompressed extends GroupKeyFullyCompressedEntryId(1319) with Deadline.FourCompressed with ValueLengthTwoCompressed
          case object DeadlineFiveCompressed extends GroupKeyFullyCompressedEntryId(1320) with Deadline.FiveCompressed with ValueLengthTwoCompressed
          case object DeadlineSixCompressed extends GroupKeyFullyCompressedEntryId(1321) with Deadline.SixCompressed with ValueLengthTwoCompressed
          case object DeadlineSevenCompressed extends GroupKeyFullyCompressedEntryId(1322) with Deadline.SevenCompressed with ValueLengthTwoCompressed
          case object DeadlineFullyCompressed extends GroupKeyFullyCompressedEntryId(1323) with Deadline.FullyCompressed with ValueLengthTwoCompressed
          case object DeadlineUncompressed extends GroupKeyFullyCompressedEntryId(1324) with Deadline.Uncompressed with ValueLengthTwoCompressed
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
          case object NoDeadline extends GroupKeyFullyCompressedEntryId(1325) with Deadline.NoDeadline with ValueLengthThreeCompressed
          case object DeadlineOneCompressed extends GroupKeyFullyCompressedEntryId(1326) with Deadline.OneCompressed with ValueLengthThreeCompressed
          case object DeadlineTwoCompressed extends GroupKeyFullyCompressedEntryId(1327) with Deadline.TwoCompressed with ValueLengthThreeCompressed
          case object DeadlineThreeCompressed extends GroupKeyFullyCompressedEntryId(1328) with Deadline.ThreeCompressed with ValueLengthThreeCompressed
          case object DeadlineFourCompressed extends GroupKeyFullyCompressedEntryId(1329) with Deadline.FourCompressed with ValueLengthThreeCompressed
          case object DeadlineFiveCompressed extends GroupKeyFullyCompressedEntryId(1330) with Deadline.FiveCompressed with ValueLengthThreeCompressed
          case object DeadlineSixCompressed extends GroupKeyFullyCompressedEntryId(1331) with Deadline.SixCompressed with ValueLengthThreeCompressed
          case object DeadlineSevenCompressed extends GroupKeyFullyCompressedEntryId(1332) with Deadline.SevenCompressed with ValueLengthThreeCompressed
          case object DeadlineFullyCompressed extends GroupKeyFullyCompressedEntryId(1333) with Deadline.FullyCompressed with ValueLengthThreeCompressed
          case object DeadlineUncompressed extends GroupKeyFullyCompressedEntryId(1334) with Deadline.Uncompressed with ValueLengthThreeCompressed
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
          case object NoDeadline extends GroupKeyFullyCompressedEntryId(1335) with Deadline.NoDeadline with ValueLengthFullyCompressed
          case object DeadlineOneCompressed extends GroupKeyFullyCompressedEntryId(1336) with Deadline.OneCompressed with ValueLengthFullyCompressed
          case object DeadlineTwoCompressed extends GroupKeyFullyCompressedEntryId(1337) with Deadline.TwoCompressed with ValueLengthFullyCompressed
          case object DeadlineThreeCompressed extends GroupKeyFullyCompressedEntryId(1338) with Deadline.ThreeCompressed with ValueLengthFullyCompressed
          case object DeadlineFourCompressed extends GroupKeyFullyCompressedEntryId(1339) with Deadline.FourCompressed with ValueLengthFullyCompressed
          case object DeadlineFiveCompressed extends GroupKeyFullyCompressedEntryId(1340) with Deadline.FiveCompressed with ValueLengthFullyCompressed
          case object DeadlineSixCompressed extends GroupKeyFullyCompressedEntryId(1341) with Deadline.SixCompressed with ValueLengthFullyCompressed
          case object DeadlineSevenCompressed extends GroupKeyFullyCompressedEntryId(1342) with Deadline.SevenCompressed with ValueLengthFullyCompressed
          case object DeadlineFullyCompressed extends GroupKeyFullyCompressedEntryId(1343) with Deadline.FullyCompressed with ValueLengthFullyCompressed
          case object DeadlineUncompressed extends GroupKeyFullyCompressedEntryId(1344) with Deadline.Uncompressed with ValueLengthFullyCompressed
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
          case object NoDeadline extends GroupKeyFullyCompressedEntryId(1345) with Deadline.NoDeadline with ValueLengthUncompressed
          case object DeadlineOneCompressed extends GroupKeyFullyCompressedEntryId(1346) with Deadline.OneCompressed with ValueLengthUncompressed
          case object DeadlineTwoCompressed extends GroupKeyFullyCompressedEntryId(1347) with Deadline.TwoCompressed with ValueLengthUncompressed
          case object DeadlineThreeCompressed extends GroupKeyFullyCompressedEntryId(1348) with Deadline.ThreeCompressed with ValueLengthUncompressed
          case object DeadlineFourCompressed extends GroupKeyFullyCompressedEntryId(1349) with Deadline.FourCompressed with ValueLengthUncompressed
          case object DeadlineFiveCompressed extends GroupKeyFullyCompressedEntryId(1350) with Deadline.FiveCompressed with ValueLengthUncompressed
          case object DeadlineSixCompressed extends GroupKeyFullyCompressedEntryId(1351) with Deadline.SixCompressed with ValueLengthUncompressed
          case object DeadlineSevenCompressed extends GroupKeyFullyCompressedEntryId(1352) with Deadline.SevenCompressed with ValueLengthUncompressed
          case object DeadlineFullyCompressed extends GroupKeyFullyCompressedEntryId(1353) with Deadline.FullyCompressed with ValueLengthUncompressed
          case object DeadlineUncompressed extends GroupKeyFullyCompressedEntryId(1354) with Deadline.Uncompressed with ValueLengthUncompressed
        }
      }
    }
  }

  def keyIdsList: List[GroupKeyFullyCompressedEntryId] = SealedList.list[GroupKeyFullyCompressedEntryId].sortBy(_.id)

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