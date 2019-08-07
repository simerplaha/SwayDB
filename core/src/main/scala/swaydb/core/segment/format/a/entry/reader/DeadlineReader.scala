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
import swaydb.core.data.KeyValue
import swaydb.core.segment.format.a.entry.id.BaseEntryId
import swaydb.core.util.Bytes
import swaydb.core.util.TimeUtil._
import swaydb.data.slice.ReaderBase
import swaydb.data.util.ByteSizeOf

import scala.annotation.implicitNotFound
import scala.concurrent.duration

@implicitNotFound("Type class implementation not found for DeadlineReader of type ${T}")
sealed trait DeadlineReader[-T] {
  def isPrefixCompressed: Boolean

  def read(indexReader: ReaderBase[swaydb.Error.Segment],
           previous: Option[KeyValue.ReadOnly]): IO[swaydb.Error.Segment, Option[duration.Deadline]]
}

object DeadlineReader {
  implicit object NoDeadlineReader extends DeadlineReader[BaseEntryId.Deadline.NoDeadline] {
    override def isPrefixCompressed: Boolean = false

    override def read(indexReader: ReaderBase[swaydb.Error.Segment],
                      previous: Option[KeyValue.ReadOnly]): IO[swaydb.Error.Segment, Option[duration.Deadline]] =
      IO.none
  }

  implicit object DeadlineFullyCompressedReader extends DeadlineReader[BaseEntryId.Deadline.FullyCompressed] {
    override def isPrefixCompressed: Boolean = true

    override def read(indexReader: ReaderBase[swaydb.Error.Segment],
                      previous: Option[KeyValue.ReadOnly]): IO[swaydb.Error.Segment, Option[duration.Deadline]] =
      previous map {
        previous =>
          previous.indexEntryDeadline map {
            deadline =>
              IO.Success(Some(deadline))
          } getOrElse IO.failed(EntryReaderFailure.NoPreviousDeadline)
      } getOrElse {
        IO.failed(EntryReaderFailure.NoPreviousKeyValue)
      }
  }

  private def decompressDeadline(indexReader: ReaderBase[swaydb.Error.Segment],
                                 commonBytes: Int,
                                 previous: Option[KeyValue.ReadOnly]): IO[swaydb.Error.Segment, Option[duration.Deadline]] =
    previous map {
      previous =>
        previous.indexEntryDeadline map {
          previousDeadline =>
            val previousDeadlineBytes = previousDeadline.toBytes
            val remainingDeadlineBytes = ByteSizeOf.long - commonBytes
            indexReader.read(remainingDeadlineBytes) flatMap {
              rightDeadlineBytes =>
                IO {
                  Bytes
                    .decompress(
                      previous = previousDeadlineBytes,
                      next = rightDeadlineBytes,
                      commonBytes = commonBytes
                    )
                    .readLong()
                    .toDeadlineOption
                }
            }
        } getOrElse {
          IO.failed(EntryReaderFailure.NoPreviousDeadline)
        }
    } getOrElse {
      IO.failed(EntryReaderFailure.NoPreviousKeyValue)
    }

  implicit object DeadlineOneCompressedReader extends DeadlineReader[BaseEntryId.Deadline.OneCompressed] {
    override def isPrefixCompressed: Boolean = true

    override def read(indexReader: ReaderBase[swaydb.Error.Segment],
                      previous: Option[KeyValue.ReadOnly]): IO[swaydb.Error.Segment, Option[duration.Deadline]] =
      decompressDeadline(indexReader = indexReader, commonBytes = 1, previous = previous)
  }

  implicit object DeadlineTwoCompressedReader extends DeadlineReader[BaseEntryId.Deadline.TwoCompressed] {
    override def isPrefixCompressed: Boolean = true

    override def read(indexReader: ReaderBase[swaydb.Error.Segment],
                      previous: Option[KeyValue.ReadOnly]): IO[swaydb.Error.Segment, Option[duration.Deadline]] =
      decompressDeadline(indexReader = indexReader, commonBytes = 2, previous = previous)
  }
  implicit object DeadlineThreeCompressedReader extends DeadlineReader[BaseEntryId.Deadline.ThreeCompressed] {
    override def isPrefixCompressed: Boolean = true

    override def read(indexReader: ReaderBase[swaydb.Error.Segment],
                      previous: Option[KeyValue.ReadOnly]): IO[swaydb.Error.Segment, Option[duration.Deadline]] =
      decompressDeadline(indexReader = indexReader, commonBytes = 3, previous = previous)
  }
  implicit object DeadlineFourCompressedReader extends DeadlineReader[BaseEntryId.Deadline.FourCompressed] {
    override def isPrefixCompressed: Boolean = true

    override def read(indexReader: ReaderBase[swaydb.Error.Segment],
                      previous: Option[KeyValue.ReadOnly]): IO[swaydb.Error.Segment, Option[duration.Deadline]] =
      decompressDeadline(indexReader = indexReader, commonBytes = 4, previous = previous)
  }
  implicit object DeadlineFiveCompressedReader extends DeadlineReader[BaseEntryId.Deadline.FiveCompressed] {
    override def isPrefixCompressed: Boolean = true

    override def read(indexReader: ReaderBase[swaydb.Error.Segment],
                      previous: Option[KeyValue.ReadOnly]): IO[swaydb.Error.Segment, Option[duration.Deadline]] =
      decompressDeadline(indexReader = indexReader, commonBytes = 5, previous = previous)
  }

  implicit object DeadlineSixCompressedReader extends DeadlineReader[BaseEntryId.Deadline.SixCompressed] {
    override def isPrefixCompressed: Boolean = true

    override def read(indexReader: ReaderBase[swaydb.Error.Segment],
                      previous: Option[KeyValue.ReadOnly]): IO[swaydb.Error.Segment, Option[duration.Deadline]] =
      decompressDeadline(indexReader = indexReader, commonBytes = 6, previous = previous)
  }

  implicit object DeadlineSevenCompressedReader extends DeadlineReader[BaseEntryId.Deadline.SevenCompressed] {
    override def isPrefixCompressed: Boolean = true

    override def read(indexReader: ReaderBase[swaydb.Error.Segment],
                      previous: Option[KeyValue.ReadOnly]): IO[swaydb.Error.Segment, Option[duration.Deadline]] =
      decompressDeadline(indexReader = indexReader, commonBytes = 7, previous = previous)
  }

  implicit object DeadlineUncompressedReader extends DeadlineReader[BaseEntryId.Deadline.Uncompressed] {
    override def isPrefixCompressed: Boolean = false

    override def read(indexReader: ReaderBase[swaydb.Error.Segment],
                      previous: Option[KeyValue.ReadOnly]): IO[swaydb.Error.Segment, Option[duration.Deadline]] =
      indexReader.readLongUnsigned() map (_.toDeadlineOption)
  }
}
