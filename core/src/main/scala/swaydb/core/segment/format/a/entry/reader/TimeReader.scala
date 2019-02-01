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

package swaydb.core.segment.format.a.entry.reader

import scala.annotation.implicitNotFound
import swaydb.data.io.IO
import swaydb.core.data.{KeyValue, Time}
import swaydb.core.segment.format.a.entry.id.EntryId
import swaydb.core.util.Bytes
import swaydb.data.slice.Reader

@implicitNotFound("Type class implementation not found for TimeReader of type ${T}")
sealed trait TimeReader[-T] {
  def read(indexReader: Reader,
           previous: Option[KeyValue.ReadOnly]): IO[Time]
}

/**
  * Time is always set for only Fixed key-values.
  * Group and Range key-values do not have time set.
  */
object TimeReader {

  implicit object NoTimeReader extends TimeReader[EntryId.Time.NoTime] {
    override def read(indexReader: Reader,
                      previous: Option[KeyValue.ReadOnly]): IO[Time] =
      Time.successEmpty
  }

  implicit object UnCompressedTimeReader extends TimeReader[EntryId.Time.Uncompressed] {
    override def read(indexReader: Reader,
                      previous: Option[KeyValue.ReadOnly]): IO[Time] =
      indexReader.readIntUnsigned() flatMap {
        timeSize =>
          indexReader.read(timeSize) map {
            time =>
              Time(time)
          }
      }
  }

  implicit object PartiallyCompressedTimeReader extends TimeReader[EntryId.Time.PartiallyCompressed] {

    def readTime(indexReader: Reader,
                 previousTime: Time): IO[Time] =
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

    override def read(indexReader: Reader,
                      previous: Option[KeyValue.ReadOnly]): IO[Time] =
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
          IO.Failure(EntryReaderFailure.PreviousIsNotFixedKeyValue)

      } getOrElse {
        IO.Failure(EntryReaderFailure.NoPreviousKeyValue)
      }
  }

  implicit object TimeFullyCompressedTimeReader extends TimeReader[EntryId.Time.FullyCompressed] {
    def readTime(indexReader: Reader,
                 previousTime: Time): IO[Time] =
      indexReader.readIntUnsigned() map {
        commonBytes =>
          Time(previousTime.time.take(commonBytes))
      }

    override def read(indexReader: Reader,
                      previous: Option[KeyValue.ReadOnly]): IO[Time] =
      previous map {
        case previous: KeyValue.ReadOnly.Put =>
          readTime(indexReader, previous.time)

        case previous: KeyValue.ReadOnly.Remove =>
          readTime(indexReader, previous.time)

        case previous: KeyValue.ReadOnly.Update =>
          readTime(indexReader, previous.time)

        case previous: KeyValue.ReadOnly.PendingApply =>
          readTime(indexReader, previous.time)

        case previous: KeyValue.ReadOnly.Function =>
          readTime(indexReader, previous.time)

        case _: KeyValue.ReadOnly.Range | _: KeyValue.ReadOnly.Group =>
          IO.Failure(EntryReaderFailure.PreviousIsNotFixedKeyValue)

      } getOrElse {
        IO.Failure(EntryReaderFailure.NoPreviousKeyValue)
      }
  }
}
