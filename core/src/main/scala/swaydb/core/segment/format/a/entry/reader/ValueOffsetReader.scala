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

package swaydb.core.segment.format.a.entry.reader

import swaydb.core.data.Persistent
import swaydb.core.segment.format.a.entry.id.EntryId
import swaydb.core.util.{Bytes, TryUtil}
import swaydb.data.slice.{Reader, Slice}
import swaydb.data.util.ByteSizeOf

import scala.annotation.implicitNotFound
import scala.util.{Failure, Try}

@implicitNotFound("Type class implementation not found for ValueOffsetReader of type ${T}")
sealed trait ValueOffsetReader[-T] {

  def read(indexReader: Reader,
           previous: Option[Persistent]): Try[Int]

}

object ValueOffsetReader {

  private def readOffset(indexReader: Reader,
                         previous: Option[Persistent],
                         commonBytes: Int): Try[Int] =
    previous.map(_.valueOffset) map {
      previousValueOffset =>
        indexReader.read(ByteSizeOf.int - commonBytes) map {
          valueOffsetBytes =>
            Bytes.decompress(Slice.writeInt(previousValueOffset), valueOffsetBytes, commonBytes).readInt()
        }
    } getOrElse {
      Failure(EntryReaderFailure.NoPreviousKeyValue)
    }

  implicit object ValueOffsetOneCompressed extends ValueOffsetReader[EntryId.ValueOffset.OneCompressed] {
    override def read(indexReader: Reader,
                      previous: Option[Persistent]): Try[Int] =
      readOffset(indexReader, previous, 1)
  }

  implicit object ValueOffsetTwoCompressed extends ValueOffsetReader[EntryId.ValueOffset.TwoCompressed] {
    override def read(indexReader: Reader,
                      previous: Option[Persistent]): Try[Int] =
      readOffset(indexReader, previous, 2)
  }

  implicit object ValueOffsetThreeCompressed extends ValueOffsetReader[EntryId.ValueOffset.ThreeCompressed] {
    override def read(indexReader: Reader,
                      previous: Option[Persistent]): Try[Int] =
      readOffset(indexReader, previous, 3)
  }

  implicit object ValueOffsetUncompressed extends ValueOffsetReader[EntryId.ValueOffset.Uncompressed] {
    override def read(indexReader: Reader,
                      previous: Option[Persistent]): Try[Int] =
      indexReader.readIntUnsigned()
  }

  implicit object ValueOffsetReaderNoValue extends ValueOffsetReader[EntryId.Value.NoValue] {
    override def read(indexReader: Reader,
                      previous: Option[Persistent]): Try[Int] =
      TryUtil.successZero
  }

  implicit object ValueOffsetReaderValueFullyCompressed extends ValueOffsetReader[EntryId.Value.FullyCompressed] {
    override def read(indexReader: Reader,
                      previous: Option[Persistent]): Try[Int] =
      TryUtil.successZero
  }
}
