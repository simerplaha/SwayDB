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

import scala.annotation.implicitNotFound
import swaydb.core.data.Persistent
import swaydb.core.segment.format.a.entry.id.BaseEntryId
import swaydb.core.util.Bytes
import swaydb.data.IO
import swaydb.data.slice.{Reader, Slice}
import swaydb.data.util.ByteSizeOf

@implicitNotFound("Type class implementation not found for ValueOffsetReader of type ${T}")
sealed trait ValueOffsetReader[-T] {
  def isPrefixCompressed: Boolean

  def read(indexReader: Reader,
           previous: Option[Persistent]): IO[Int]
}

object ValueOffsetReader {

  private def readOffset(indexReader: Reader,
                         previous: Option[Persistent],
                         commonBytes: Int): IO[Int] =
    previous.map(_.valueOffset) map {
      previousValueOffset =>
        indexReader.read(ByteSizeOf.int - commonBytes) map {
          valueOffsetBytes =>
            Bytes.decompress(Slice.writeInt(previousValueOffset), valueOffsetBytes, commonBytes).readInt()
        }
    } getOrElse {
      IO.Failure(EntryReaderFailure.NoPreviousKeyValue)
    }

  implicit object ValueOffsetOneCompressed extends ValueOffsetReader[BaseEntryId.ValueOffset.OneCompressed] {
    override def isPrefixCompressed: Boolean = true

    override def read(indexReader: Reader,
                      previous: Option[Persistent]): IO[Int] =
      readOffset(indexReader, previous, 1)
  }

  implicit object ValueOffsetTwoCompressed extends ValueOffsetReader[BaseEntryId.ValueOffset.TwoCompressed] {
    override def isPrefixCompressed: Boolean = true

    override def read(indexReader: Reader,
                      previous: Option[Persistent]): IO[Int] =
      readOffset(indexReader, previous, 2)
  }

  implicit object ValueOffsetThreeCompressed extends ValueOffsetReader[BaseEntryId.ValueOffset.ThreeCompressed] {
    override def isPrefixCompressed: Boolean = true

    override def read(indexReader: Reader,
                      previous: Option[Persistent]): IO[Int] =
      readOffset(indexReader, previous, 3)
  }

  implicit object ValueOffsetUncompressed extends ValueOffsetReader[BaseEntryId.ValueOffset.Uncompressed] {
    override def isPrefixCompressed: Boolean = false

    override def read(indexReader: Reader,
                      previous: Option[Persistent]): IO[Int] =
      indexReader.readIntUnsigned()
  }

  implicit object ValueOffsetReaderValueOffsetFullyCompressed extends ValueOffsetReader[BaseEntryId.ValueOffset.FullyCompressed] {
    override def isPrefixCompressed: Boolean = true

    override def read(indexReader: Reader,
                      previous: Option[Persistent]): IO[Int] =
      IO.zero
  }

  implicit object ValueOffsetReaderNoValue extends ValueOffsetReader[BaseEntryId.Value.NoValue] {
    override def isPrefixCompressed: Boolean = false

    override def read(indexReader: Reader,
                      previous: Option[Persistent]): IO[Int] =
      IO.zero
  }

}
