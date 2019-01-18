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

import swaydb.core.data.KeyValue
import swaydb.core.segment.format.a.entry.id.EntryId
import swaydb.core.util.Bytes
import swaydb.data.slice.{Reader, Slice}

import scala.annotation.implicitNotFound
import scala.util.{Failure, Try}

@implicitNotFound("Type class implementation not found for KeyReader of type ${T}")
sealed trait KeyReader[-T] {
  def read(indexReader: Reader,
           previous: Option[KeyValue.ReadOnly]): Try[Slice[Byte]]
}

object KeyReader {

  implicit object UnCompressedKeyReader extends KeyReader[EntryId.Key.Uncompressed] {
    override def read(indexReader: Reader,
                      previous: Option[KeyValue.ReadOnly]): Try[Slice[Byte]] =
      indexReader.readRemaining()
  }

  implicit object PartiallyCompressedKeyReader extends KeyReader[EntryId.Key.PartiallyCompressed] {
    override def read(indexReader: Reader,
                      previous: Option[KeyValue.ReadOnly]): Try[Slice[Byte]] =
      previous map {
        previous =>
          indexReader.readIntUnsigned() flatMap {
            commonBytes =>
              indexReader.readRemaining() map {
                rightBytes =>
                  Bytes.decompress(previous.key, rightBytes, commonBytes)
              }
          }
      } getOrElse {
        Failure(EntryReaderFailure.NoPreviousKeyValue)
      }
  }

  implicit object KeyFullyCompressedReader extends KeyReader[EntryId.Key.FullyCompressed] {
    override def read(indexReader: Reader,
                      previous: Option[KeyValue.ReadOnly]): Try[Slice[Byte]] =
      previous map {
        previous =>
          indexReader.readIntUnsigned() map {
            commonBytes =>
              previous.key.take(commonBytes)
          }
      } getOrElse {
        Failure(EntryReaderFailure.NoPreviousKeyValue)
      }
  }
}
