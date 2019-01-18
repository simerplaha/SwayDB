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
import swaydb.core.util.TryUtil
import swaydb.data.slice.Reader

import scala.annotation.implicitNotFound
import scala.util.{Failure, Success, Try}

@implicitNotFound("Type class implementation not found for ValueReader of type ${T}")
sealed trait ValueReader[-T] {

  def read[V](indexReader: Reader,
              previous: Option[Persistent])(implicit valueOffsetReader: ValueOffsetReader[V],
                                            valueLengthReader: ValueLengthReader[V]): Try[Option[(Int, Int)]]

}

object ValueReader {
  implicit object NoValueReader extends ValueReader[EntryId.Value.NoValue] {
    override def read[V](indexReader: Reader,
                         previous: Option[Persistent])(implicit valueOffsetReader: ValueOffsetReader[V],
                                                       valueLengthReader: ValueLengthReader[V]): Try[Option[(Int, Int)]] =
      TryUtil.successNone
  }

  implicit object ValueUncompressedReader extends ValueReader[EntryId.Value.Uncompressed] {
    override def read[V](indexReader: Reader,
                         previous: Option[Persistent])(implicit valueOffsetReader: ValueOffsetReader[V],
                                                       valueLengthReader: ValueLengthReader[V]): Try[Option[(Int, Int)]] =
      valueOffsetReader.read(indexReader, previous) flatMap {
        valueOffset =>
          valueLengthReader.read(indexReader, previous) map {
            valueLength =>
              Some((valueOffset, valueLength))
          }
      }
  }

  implicit object ValueFullyCompressedReader extends ValueReader[EntryId.Value.FullyCompressed] {
    override def read[V](indexReader: Reader,
                         previous: Option[Persistent])(implicit valueOffsetReader: ValueOffsetReader[V],
                                                       valueLengthReader: ValueLengthReader[V]): Try[Option[(Int, Int)]] =
      previous map {
        previous =>
          Success(Some((previous.valueOffset, previous.valueLength)))
      } getOrElse {
        Failure(EntryReaderFailure.NoPreviousKeyValue)
      }
  }
}
