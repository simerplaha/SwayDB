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

import swaydb.IO
import swaydb.core.data.Persistent
import swaydb.core.segment.format.a.entry.id.BaseEntryId
import swaydb.data.io.Core
import swaydb.data.slice.Reader

import scala.annotation.implicitNotFound
import swaydb.data.io.Core.IO.Error.ErrorHandler

@implicitNotFound("Type class implementation not found for ValueReader of type ${T}")
sealed trait ValueReader[-T] {

  def isPrefixCompressed: Boolean

  def read[V](indexReader: Reader[Core.IO.Error],
              previous: Option[Persistent])(implicit valueOffsetReader: ValueOffsetReader[V],
                                            valueLengthReader: ValueLengthReader[V]): IO[Core.IO.Error, Option[(Int, Int)]]
}

object ValueReader {
  implicit object NoValueReader extends ValueReader[BaseEntryId.Value.NoValue] {
    override def isPrefixCompressed: Boolean = false

    override def read[V](indexReader: Reader[Core.IO.Error],
                         previous: Option[Persistent])(implicit valueOffsetReader: ValueOffsetReader[V],
                                                       valueLengthReader: ValueLengthReader[V]): IO[Core.IO.Error, Option[(Int, Int)]] =
      IO.none
  }

  implicit object ValueUncompressedReader extends ValueReader[BaseEntryId.Value.Uncompressed] {
    override def isPrefixCompressed: Boolean = false
    override def read[V](indexReader: Reader[Core.IO.Error],
                         previous: Option[Persistent])(implicit valueOffsetReader: ValueOffsetReader[V],
                                                       valueLengthReader: ValueLengthReader[V]): IO[Core.IO.Error, Option[(Int, Int)]] =
      valueOffsetReader.read(indexReader, previous) flatMap {
        valueOffset =>
          valueLengthReader.read(indexReader, previous) map {
            valueLength =>
              Some((valueOffset, valueLength))
          }
      }
  }

  implicit object ValueFullyCompressedReader extends ValueReader[BaseEntryId.Value.FullyCompressed] {
    override def isPrefixCompressed: Boolean = true

    override def read[V](indexReader: Reader[Core.IO.Error],
                         previous: Option[Persistent])(implicit valueOffsetReader: ValueOffsetReader[V],
                                                       valueLengthReader: ValueLengthReader[V]): IO[Core.IO.Error, Option[(Int, Int)]] =
      ValueUncompressedReader.read(
        indexReader = indexReader,
        previous = previous
      )
  }
}
