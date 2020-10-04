/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.core.segment.format.a.entry.reader

import swaydb.core.data.PersistentOption
import swaydb.core.segment.format.a.entry.id.BaseEntryId
import swaydb.data.slice.ReaderBase
import swaydb.data.util.TupleOrNone

import scala.annotation.implicitNotFound

@implicitNotFound("Type class implementation not found for ValueReader of type ${T}")
sealed trait ValueReader[-T] {

  def isPrefixCompressed: Boolean

  def read[V](indexReader: ReaderBase[Byte],
              previous: PersistentOption,
              valueOffsetReader: ValueOffsetReader[V],
              valueLengthReader: ValueLengthReader[V]): TupleOrNone[Int, Int]
}

object ValueReader {
  implicit object NoValueReader extends ValueReader[BaseEntryId.Value.NoValue] {
    override def isPrefixCompressed: Boolean = false

    override def read[V](indexReader: ReaderBase[Byte],
                         previous: PersistentOption,
                         valueOffsetReader: ValueOffsetReader[V],
                         valueLengthReader: ValueLengthReader[V]): TupleOrNone[Int, Int] =
      TupleOrNone.None
  }

  implicit object ValueUncompressedReader extends ValueReader[BaseEntryId.Value.Uncompressed] {
    override def isPrefixCompressed: Boolean = false
    override def read[V](indexReader: ReaderBase[Byte],
                         previous: PersistentOption,
                         valueOffsetReader: ValueOffsetReader[V],
                         valueLengthReader: ValueLengthReader[V]): TupleOrNone[Int, Int] = {
      val valueOffset = valueOffsetReader.read(indexReader, previous)
      val valueLength = valueLengthReader.read(indexReader, previous)
      TupleOrNone.Some(valueOffset, valueLength)
    }
  }

  implicit object ValueFullyCompressedReader extends ValueReader[BaseEntryId.Value.FullyCompressed] {
    //prefixCompression does not apply on the value itself since it can still hold reference to offset and length.
    //A value is considered prefix compressed only if it's valueOffset and valueLength are prefix compressed.
    override def isPrefixCompressed: Boolean = false

    override def read[V](indexReader: ReaderBase[Byte],
                         previous: PersistentOption,
                         valueOffsetReader: ValueOffsetReader[V],
                         valueLengthReader: ValueLengthReader[V]): TupleOrNone[Int, Int] =
      ValueUncompressedReader.read(
        indexReader = indexReader,
        previous = previous,
        valueOffsetReader = valueOffsetReader,
        valueLengthReader = valueLengthReader
      )
  }
}
