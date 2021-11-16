/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb.core.segment.entry.reader

import swaydb.core.data.{Persistent, PersistentOption}
import swaydb.core.segment.entry.id.BaseEntryId
import swaydb.core.util.Bytes
import swaydb.core.util.Times._
import swaydb.slice.ReaderBase
import swaydb.utils.ByteSizeOf

import scala.annotation.implicitNotFound
import scala.concurrent.duration

@implicitNotFound("Type class implementation not found for DeadlineReader of type ${T}")
sealed trait DeadlineReader[-T] {
  def isPrefixCompressed: Boolean

  def read(indexReader: ReaderBase[Byte],
           previous: PersistentOption): Option[duration.Deadline]
}

object DeadlineReader {
  implicit object NoDeadlineReader extends DeadlineReader[BaseEntryId.Deadline.NoDeadline] {
    override def isPrefixCompressed: Boolean = false

    override def read(indexReader: ReaderBase[Byte],
                      previous: PersistentOption): Option[duration.Deadline] =
      None
  }

  implicit object DeadlineFullyCompressedReader extends DeadlineReader[BaseEntryId.Deadline.FullyCompressed] {
    override def isPrefixCompressed: Boolean = true

    override def read(indexReader: ReaderBase[Byte],
                      previous: PersistentOption): Option[duration.Deadline] =
      previous match {
        case previous: Persistent =>
          previous.indexEntryDeadline match {
            case some @ Some(_) =>
              some
            case None =>
              throw EntryReaderFailure.NoPreviousDeadline
          }

        case Persistent.Null =>
          throw EntryReaderFailure.NoPreviousKeyValue
      }
  }

  private def decompressDeadline(indexReader: ReaderBase[Byte],
                                 commonBytes: Int,
                                 previous: PersistentOption): Option[duration.Deadline] =
    previous match {
      case previous: Persistent =>
        previous.indexEntryDeadline match {
          case Some(previousDeadline) =>
            Bytes
              .decompress(
                previous = previousDeadline.toBytes,
                next = indexReader.read(ByteSizeOf.long - commonBytes),
                commonBytes = commonBytes
              )
              .readLong()
              .toDeadlineOption

          case None =>
            throw EntryReaderFailure.NoPreviousDeadline
        }

      case Persistent.Null =>
        throw EntryReaderFailure.NoPreviousKeyValue
    }

  implicit object DeadlineOneCompressedReader extends DeadlineReader[BaseEntryId.Deadline.OneCompressed] {
    override def isPrefixCompressed: Boolean = true

    override def read(indexReader: ReaderBase[Byte],
                      previous: PersistentOption): Option[duration.Deadline] =
      decompressDeadline(indexReader = indexReader, commonBytes = 1, previous = previous)
  }

  implicit object DeadlineTwoCompressedReader extends DeadlineReader[BaseEntryId.Deadline.TwoCompressed] {
    override def isPrefixCompressed: Boolean = true

    override def read(indexReader: ReaderBase[Byte],
                      previous: PersistentOption): Option[duration.Deadline] =
      decompressDeadline(indexReader = indexReader, commonBytes = 2, previous = previous)
  }

  implicit object DeadlineThreeCompressedReader extends DeadlineReader[BaseEntryId.Deadline.ThreeCompressed] {
    override def isPrefixCompressed: Boolean = true

    override def read(indexReader: ReaderBase[Byte],
                      previous: PersistentOption): Option[duration.Deadline] =
      decompressDeadline(indexReader = indexReader, commonBytes = 3, previous = previous)
  }

  implicit object DeadlineFourCompressedReader extends DeadlineReader[BaseEntryId.Deadline.FourCompressed] {
    override def isPrefixCompressed: Boolean = true

    override def read(indexReader: ReaderBase[Byte],
                      previous: PersistentOption): Option[duration.Deadline] =
      decompressDeadline(indexReader = indexReader, commonBytes = 4, previous = previous)
  }

  implicit object DeadlineFiveCompressedReader extends DeadlineReader[BaseEntryId.Deadline.FiveCompressed] {
    override def isPrefixCompressed: Boolean = true

    override def read(indexReader: ReaderBase[Byte],
                      previous: PersistentOption): Option[duration.Deadline] =
      decompressDeadline(indexReader = indexReader, commonBytes = 5, previous = previous)
  }

  implicit object DeadlineSixCompressedReader extends DeadlineReader[BaseEntryId.Deadline.SixCompressed] {
    override def isPrefixCompressed: Boolean = true

    override def read(indexReader: ReaderBase[Byte],
                      previous: PersistentOption): Option[duration.Deadline] =
      decompressDeadline(indexReader = indexReader, commonBytes = 6, previous = previous)
  }

  implicit object DeadlineSevenCompressedReader extends DeadlineReader[BaseEntryId.Deadline.SevenCompressed] {
    override def isPrefixCompressed: Boolean = true

    override def read(indexReader: ReaderBase[Byte],
                      previous: PersistentOption): Option[duration.Deadline] =
      decompressDeadline(indexReader = indexReader, commonBytes = 7, previous = previous)
  }

  implicit object DeadlineUncompressedReader extends DeadlineReader[BaseEntryId.Deadline.Uncompressed] {
    override def isPrefixCompressed: Boolean = false

    override def read(indexReader: ReaderBase[Byte],
                      previous: PersistentOption): Option[duration.Deadline] =
      indexReader.readUnsignedLong().toDeadlineOption
  }
}
