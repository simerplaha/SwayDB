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

import swaydb.core.data.PersistentOption
import swaydb.core.segment.entry.id.BaseEntryId
import swaydb.slice.ReaderBase
import swaydb.utils.TupleOrNone

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
