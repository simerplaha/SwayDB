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
import scala.concurrent.duration
import swaydb.core.data.KeyValue
import swaydb.core.segment.format.a.entry.id.EntryId
import swaydb.core.util.Bytes
import swaydb.core.util.TimeUtil._
import swaydb.data.io.IO
import swaydb.data.slice.Reader
import swaydb.data.util.ByteSizeOf

@implicitNotFound("Type class implementation not found for DeadlineReader of type ${T}")
sealed trait DeadlineReader[-T] {
  def read(indexReader: Reader,
           previous: Option[KeyValue.ReadOnly]): IO[Option[duration.Deadline]]
}

object DeadlineReader {
  implicit object NoDeadlineReader extends DeadlineReader[EntryId.Deadline.NoDeadline] {
    override def read(indexReader: Reader,
                      previous: Option[KeyValue.ReadOnly]): IO[Option[duration.Deadline]] =
      IO.successNone
  }

  implicit object DeadlineFullyCompressedReader extends DeadlineReader[EntryId.Deadline.FullyCompressed] {
    override def read(indexReader: Reader,
                      previous: Option[KeyValue.ReadOnly]): IO[Option[duration.Deadline]] =
      previous map {
        previous =>
          previous.indexEntryDeadline map {
            deadline =>
              IO.Sync(Some(deadline))
          } getOrElse IO.Failure(EntryReaderFailure.NoPreviousDeadline)
      } getOrElse {
        IO.Failure(EntryReaderFailure.NoPreviousKeyValue)
      }
  }

  private def decompressDeadline(indexReader: Reader,
                                 commonBytes: Int,
                                 previous: Option[KeyValue.ReadOnly]): IO[Option[duration.Deadline]] =
    previous map {
      previous =>
        previous.indexEntryDeadline map {
          previousDeadline =>
            val previousDeadlineBytes = previousDeadline.toBytes
            val remainingDeadlineBytes = ByteSizeOf.long - commonBytes
            indexReader.read(remainingDeadlineBytes) flatMap {
              rightDeadlineBytes =>
                IO {
                  Bytes.decompress(previousDeadlineBytes, rightDeadlineBytes, commonBytes)
                    .readLong().toDeadlineOption
                }
            }
        } getOrElse {
          IO.Failure(EntryReaderFailure.NoPreviousDeadline)
        }
    } getOrElse {
      IO.Failure(EntryReaderFailure.NoPreviousKeyValue)
    }

  implicit object DeadlineOneCompressedReader extends DeadlineReader[EntryId.Deadline.OneCompressed] {
    override def read(indexReader: Reader,
                      previous: Option[KeyValue.ReadOnly]): IO[Option[duration.Deadline]] =
      decompressDeadline(indexReader = indexReader, commonBytes = 1, previous = previous)
  }

  implicit object DeadlineTwoCompressedReader extends DeadlineReader[EntryId.Deadline.TwoCompressed] {
    override def read(indexReader: Reader,
                      previous: Option[KeyValue.ReadOnly]): IO[Option[duration.Deadline]] =
      decompressDeadline(indexReader = indexReader, commonBytes = 2, previous = previous)
  }
  implicit object DeadlineThreeCompressedReader extends DeadlineReader[EntryId.Deadline.ThreeCompressed] {
    override def read(indexReader: Reader,
                      previous: Option[KeyValue.ReadOnly]): IO[Option[duration.Deadline]] =
      decompressDeadline(indexReader = indexReader, commonBytes = 3, previous = previous)
  }
  implicit object DeadlineFourCompressedReader extends DeadlineReader[EntryId.Deadline.FourCompressed] {
    override def read(indexReader: Reader,
                      previous: Option[KeyValue.ReadOnly]): IO[Option[duration.Deadline]] =
      decompressDeadline(indexReader = indexReader, commonBytes = 4, previous = previous)
  }
  implicit object DeadlineFiveCompressedReader extends DeadlineReader[EntryId.Deadline.FiveCompressed] {
    override def read(indexReader: Reader,
                      previous: Option[KeyValue.ReadOnly]): IO[Option[duration.Deadline]] =
      decompressDeadline(indexReader = indexReader, commonBytes = 5, previous = previous)
  }

  implicit object DeadlineSixCompressedReader extends DeadlineReader[EntryId.Deadline.SixCompressed] {
    override def read(indexReader: Reader,
                      previous: Option[KeyValue.ReadOnly]): IO[Option[duration.Deadline]] =
      decompressDeadline(indexReader = indexReader, commonBytes = 6, previous = previous)
  }

  implicit object DeadlineSevenCompressedReader extends DeadlineReader[EntryId.Deadline.SevenCompressed] {
    override def read(indexReader: Reader,
                      previous: Option[KeyValue.ReadOnly]): IO[Option[duration.Deadline]] =
      decompressDeadline(indexReader = indexReader, commonBytes = 7, previous = previous)
  }

  implicit object DeadlineUncompressedReader extends DeadlineReader[EntryId.Deadline.Uncompressed] {
    override def read(indexReader: Reader,
                      previous: Option[KeyValue.ReadOnly]): IO[Option[duration.Deadline]] =
      indexReader.readLongUnsigned() map (_.toDeadlineOption)
  }
}
