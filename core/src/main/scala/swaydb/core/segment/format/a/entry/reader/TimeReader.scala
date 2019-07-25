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

package swaydb.core.segment.format.a.entry.reader

import swaydb.Error.Segment.ErrorHandler
import swaydb.IO
import swaydb.core.data.{KeyValue, Time}
import swaydb.core.segment.format.a.entry.id.BaseEntryId
import swaydb.core.util.Bytes
import swaydb.data.slice.Reader

import scala.annotation.implicitNotFound

@implicitNotFound("Type class implementation not found for TimeReader of type ${T}")
sealed trait TimeReader[-T] {
  def isPrefixCompressed: Boolean

  def read(indexReader: Reader[swaydb.Error.Segment],
           previous: Option[KeyValue.ReadOnly]): IO[swaydb.Error.Segment, Time]
}

/**
 * Time is always set for only Fixed key-values.
 * Group and Range key-values do not have time set.
 */
object TimeReader {

  implicit object NoTimeReader extends TimeReader[BaseEntryId.Time.NoTime] {
    override def isPrefixCompressed: Boolean = false

    override def read(indexReader: Reader[swaydb.Error.Segment],
                      previous: Option[KeyValue.ReadOnly]): IO[swaydb.Error.Segment, Time] =
      Time.successEmpty
  }

  implicit object UnCompressedTimeReader extends TimeReader[BaseEntryId.Time.Uncompressed] {
    override def isPrefixCompressed: Boolean = false

    override def read(indexReader: Reader[swaydb.Error.Segment],
                      previous: Option[KeyValue.ReadOnly]): IO[swaydb.Error.Segment, Time] =
      indexReader.readIntUnsigned() flatMap {
        timeSize =>
          indexReader.read(timeSize) map {
            time =>
              Time(time)
          }
      }
  }

  implicit object PartiallyCompressedTimeReader extends TimeReader[BaseEntryId.Time.PartiallyCompressed] {

    override def isPrefixCompressed: Boolean = true

    def readTime(indexReader: Reader[swaydb.Error.Segment],
                 previousTime: Time): IO[swaydb.Error.Segment, Time] =
      indexReader.readIntUnsigned() flatMap {
        commonBytes =>
          indexReader.readIntUnsigned() flatMap {
            uncompressedBytes =>
              indexReader.read(uncompressedBytes) map {
                rightBytes =>
                  val timeBytes = Bytes.decompress(previousTime.time, rightBytes, commonBytes)
                  Time(timeBytes)
              }
          }
      }

    override def read(indexReader: Reader[swaydb.Error.Segment],
                      previous: Option[KeyValue.ReadOnly]): IO[swaydb.Error.Segment, Time] =
      previous map {
        case previous: KeyValue.ReadOnly.Put =>
          readTime(indexReader, previous.time)

        case previous: KeyValue.ReadOnly.Remove =>
          readTime(indexReader, previous.time)

        case previous: KeyValue.ReadOnly.Function =>
          readTime(indexReader, previous.time)

        case previous: KeyValue.ReadOnly.PendingApply =>
          readTime(indexReader, previous.time)

        case previous: KeyValue.ReadOnly.Update =>
          readTime(indexReader, previous.time)

        case _: KeyValue.ReadOnly.Range | _: KeyValue.ReadOnly.Group =>
          IO.failed(EntryReaderFailure.PreviousIsNotFixedKeyValue)
      } getOrElse {
        IO.failed(EntryReaderFailure.NoPreviousKeyValue)
      }
  }
}
