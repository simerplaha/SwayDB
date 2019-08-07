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
import swaydb.core.data.Persistent
import swaydb.core.segment.format.a.entry.id.BaseEntryId
import swaydb.core.util.Bytes
import swaydb.data.slice.{ReaderBase, Slice}
import swaydb.data.util.ByteSizeOf

import scala.annotation.implicitNotFound

@implicitNotFound("Type class implementation not found for ValueLengthReader of type ${T}")
sealed trait ValueLengthReader[-T] {
  def isPrefixCompressed: Boolean

  def read(indexReader: ReaderBase[swaydb.Error.Segment],
           previous: Option[Persistent]): IO[swaydb.Error.Segment, Int]
}

object ValueLengthReader {

  private def readLength(indexReader: ReaderBase[swaydb.Error.Segment],
                         previous: Option[Persistent],
                         commonBytes: Int): IO[swaydb.Error.Segment, Int] =
    previous.map(_.valueLength) map {
      previousValueLength =>
        indexReader.read(ByteSizeOf.int - commonBytes) map {
          valueLengthBytes =>
            Bytes.decompress(Slice.writeInt(previousValueLength), valueLengthBytes, commonBytes).readInt()
        }
    } getOrElse {
      IO.failed(EntryReaderFailure.NoPreviousKeyValue)
    }

  implicit object ValueLengthOneCompressed extends ValueLengthReader[BaseEntryId.ValueLength.OneCompressed] {
    override def isPrefixCompressed: Boolean = true

    override def read(indexReader: ReaderBase[swaydb.Error.Segment],
                      previous: Option[Persistent]): IO[swaydb.Error.Segment, Int] =
      readLength(indexReader, previous, 1)
  }

  implicit object ValueLengthTwoCompressed extends ValueLengthReader[BaseEntryId.ValueLength.TwoCompressed] {
    override def isPrefixCompressed: Boolean = true

    override def read(indexReader: ReaderBase[swaydb.Error.Segment],
                      previous: Option[Persistent]): IO[swaydb.Error.Segment, Int] =
      readLength(indexReader, previous, 2)
  }

  implicit object ValueLengthThreeCompressed extends ValueLengthReader[BaseEntryId.ValueLength.ThreeCompressed] {
    override def isPrefixCompressed: Boolean = true

    override def read(indexReader: ReaderBase[swaydb.Error.Segment],
                      previous: Option[Persistent]): IO[swaydb.Error.Segment, Int] =
      readLength(indexReader, previous, 3)
  }

  implicit object ValueLengthFullyCompressed extends ValueLengthReader[BaseEntryId.ValueLength.FullyCompressed] {
    override def isPrefixCompressed: Boolean = true

    override def read(indexReader: ReaderBase[swaydb.Error.Segment],
                      previous: Option[Persistent]): IO[swaydb.Error.Segment, Int] =
      previous map {
        previous =>
          IO.Success(previous.valueLength)
      } getOrElse IO.failed(EntryReaderFailure.NoPreviousKeyValue)
  }

  implicit object ValueLengthUncompressed extends ValueLengthReader[BaseEntryId.ValueLength.Uncompressed] {
    override def isPrefixCompressed: Boolean = false

    override def read(indexReader: ReaderBase[swaydb.Error.Segment],
                      previous: Option[Persistent]): IO[swaydb.Error.Segment, Int] =
      indexReader.readIntUnsigned()
  }

  implicit object NoValue extends ValueLengthReader[BaseEntryId.Value.NoValue] {
    override def isPrefixCompressed: Boolean = false

    override def read(indexReader: ReaderBase[swaydb.Error.Segment],
                      previous: Option[Persistent]): IO[swaydb.Error.Segment, Int] =
      IO.zero
  }
}
