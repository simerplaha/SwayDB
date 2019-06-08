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

object BaseEntryReader3 extends BaseEntryReader {

  def read[T](id: Int,
              indexReader: Reader,
              valueReader: Reader,
              indexOffset: Int,
              nextIndexOffset: Int,
              nextIndexSize: Int,
              previous: Option[Persistent],
              reader: EntryReader[T]): Option[IO[T]] =
  //GENERATED CONDITIONS
    if (id == 247)
      Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
    else if (id < 247)
      if (id == 222)
        Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
      else if (id < 222)
        if (id == 210)
          Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 210)
          if (id == 204)
            Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.NoValue.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 204)
            if (id == 201)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.NoValue.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 201)
              if (id == 199)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 199)
                if (id == 198)
                  Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueUncompressed.ValueOffsetUncompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 199)
                if (id == 200)
                  Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.NoValue.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 201)
              if (id == 202)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.NoValue.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 203)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.NoValue.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 204)
            if (id == 207)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.NoValue.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 207)
              if (id == 205)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.NoValue.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 206)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.NoValue.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 207)
              if (id == 208)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.NoValue.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 209)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.NoValue.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 210)
          if (id == 216)
            Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 216)
            if (id == 213)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 213)
              if (id == 211)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 212)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 213)
              if (id == 214)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 215)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 216)
            if (id == 219)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 219)
              if (id == 217)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 218)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.TimePartiallyCompressed.ValueFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 219)
              if (id == 220)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 221)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else
          None
      else if (id > 222)
        if (id == 235)
          Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 235)
          if (id == 229)
            Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 229)
            if (id == 226)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 226)
              if (id == 224)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 224)
                if (id == 223)
                  Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 224)
                if (id == 225)
                  Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 226)
              if (id == 227)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 228)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 229)
            if (id == 232)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 232)
              if (id == 230)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 231)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 232)
              if (id == 233)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 234)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 235)
          if (id == 241)
            Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 241)
            if (id == 238)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 238)
              if (id == 236)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 237)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 238)
              if (id == 239)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 240)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 241)
            if (id == 244)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 244)
              if (id == 242)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 243)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 244)
              if (id == 245)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 246)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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
    else if (id > 247)
      if (id == 272)
        Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
      else if (id < 272)
        if (id == 260)
          Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 260)
          if (id == 254)
            Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 254)
            if (id == 251)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 251)
              if (id == 249)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 249)
                if (id == 248)
                  Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthThreeCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 249)
                if (id == 250)
                  Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 251)
              if (id == 252)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 253)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 254)
            if (id == 257)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 257)
              if (id == 255)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 256)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 257)
              if (id == 258)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 259)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthFullyCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 260)
          if (id == 266)
            Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 266)
            if (id == 263)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 263)
              if (id == 261)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 262)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 263)
              if (id == 264)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 265)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 266)
            if (id == 269)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 269)
              if (id == 267)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 268)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetOneCompressed.ValueLengthUncompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 269)
              if (id == 270)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 271)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else
          None
      else if (id > 272)
        if (id == 285)
          Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
        else if (id < 285)
          if (id == 279)
            Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 279)
            if (id == 276)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 276)
              if (id == 274)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id < 274)
                if (id == 273)
                  Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else if (id > 274)
                if (id == 275)
                  Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
                else
                  None
              else
                None
            else if (id > 276)
              if (id == 277)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 278)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthOneCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 279)
            if (id == 282)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 282)
              if (id == 280)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 281)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 282)
              if (id == 283)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 284)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else
            None
        else if (id > 285)
          if (id == 291)
            Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineOneCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
          else if (id < 291)
            if (id == 288)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineFullyCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 288)
              if (id == 286)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 287)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineSevenCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 288)
              if (id == 289)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthTwoCompressed.DeadlineUncompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 290)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.NoDeadline, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else
              None
          else if (id > 291)
            if (id == 294)
              Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFourCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
            else if (id < 294)
              if (id == 292)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineTwoCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 293)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineThreeCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else
                None
            else if (id > 294)
              if (id == 295)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineFiveCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
              else if (id == 296)
                Some(reader(BaseEntryId.FormatA.KeyPartiallyCompressed.NoTime.ValueUncompressed.ValueOffsetTwoCompressed.ValueLengthThreeCompressed.DeadlineSixCompressed, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))
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

  val minID = 198
  val maxID = 296
}