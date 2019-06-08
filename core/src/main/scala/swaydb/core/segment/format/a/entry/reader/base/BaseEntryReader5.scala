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

package swaydb.core.segment.format.a.entry.reader.base

import swaydb.core.data.Persistent
import swaydb.core.segment.format.a.entry.id.BaseEntryId
import swaydb.core.segment.format.a.entry.reader.EntryReader
import swaydb.data.IO
import swaydb.data.slice.Reader

object BaseEntryReader5 extends BaseEntryReader {

  def read[T](id: Int,
              indexReader: Reader,
              valueReader: Reader,
              indexOffset: Int,
              nextIndexOffset: Int,
              nextIndexSize: Int,
              previous: Option[Persistent],
              reader: EntryReader[T]): Option[IO[T]] =
  //GENERATED CONDITIONS
    if (id == 445)
      Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
    else if (id < 445)
      if (id == 420)
        Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.NoValue.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
      else if (id < 420)
        if (id == 408)
          Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 408)
          if (id == 402)
            Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 402)
            if (id == 399)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 399)
              if (id == 397)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 397)
                if (id == 396)
                  Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 397)
                if (id == 398)
                  Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 399)
              if (id == 400)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 401)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 402)
            if (id == 405)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 405)
              if (id == 403)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 404)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 405)
              if (id == 406)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 407)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 408)
          if (id == 414)
            Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 414)
            if (id == 411)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 411)
              if (id == 409)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 410)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 411)
              if (id == 412)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 413)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 414)
            if (id == 417)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 417)
              if (id == 415)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 416)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 417)
              if (id == 418)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 419)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else
          None
      else if (id > 420)
        if (id == 433)
          Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 433)
          if (id == 427)
            Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.NoValue.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 427)
            if (id == 424)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.NoValue.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 424)
              if (id == 422)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.NoValue.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 422)
                if (id == 421)
                  Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.NoValue.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 422)
                if (id == 423)
                  Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.NoValue.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 424)
              if (id == 425)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.NoValue.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 426)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.NoValue.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 427)
            if (id == 430)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 430)
              if (id == 428)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.NoValue.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 429)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.NoValue.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 430)
              if (id == 431)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 432)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 433)
          if (id == 439)
            Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 439)
            if (id == 436)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 436)
              if (id == 434)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 435)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 436)
              if (id == 437)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 438)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 439)
            if (id == 442)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 442)
              if (id == 440)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 441)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 442)
              if (id == 443)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 444)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else
          None
      else
        None
    else if (id > 445)
      if (id == 470)
        Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
      else if (id < 470)
        if (id == 458)
          Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 458)
          if (id == 452)
            Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 452)
            if (id == 449)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 449)
              if (id == 447)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 447)
                if (id == 446)
                  Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 447)
                if (id == 448)
                  Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 449)
              if (id == 450)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 451)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 452)
            if (id == 455)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 455)
              if (id == 453)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 454)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 455)
              if (id == 456)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 457)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 458)
          if (id == 464)
            Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 464)
            if (id == 461)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 461)
              if (id == 459)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 460)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 461)
              if (id == 462)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 463)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 464)
            if (id == 467)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 467)
              if (id == 465)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 466)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 467)
              if (id == 468)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 469)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else
          None
      else if (id > 470)
        if (id == 483)
          Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 483)
          if (id == 477)
            Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 477)
            if (id == 474)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 474)
              if (id == 472)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 472)
                if (id == 471)
                  Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 472)
                if (id == 473)
                  Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 474)
              if (id == 475)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 476)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 477)
            if (id == 480)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 480)
              if (id == 478)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 479)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 480)
              if (id == 481)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 482)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 483)
          if (id == 489)
            Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 489)
            if (id == 486)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 486)
              if (id == 484)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 485)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 486)
              if (id == 487)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 488)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 489)
            if (id == 492)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 492)
              if (id == 490)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 491)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 492)
              if (id == 493)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 494)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimeUncompressed.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else
          None
      else
        None
    else
      None

  val minID = 396
  val maxID = 494
}