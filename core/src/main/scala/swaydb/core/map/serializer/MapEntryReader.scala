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

package swaydb.core.map.serializer

import swaydb.IO
import swaydb.core.io.reader.Reader
import swaydb.core.map.MapEntry
import swaydb.data.slice.{Reader, Slice}

import scala.annotation.implicitNotFound

@implicitNotFound("Type class implementation not found for MapEntryReader of type ${T}")
trait MapEntryReader[T <: MapEntry[_, _]] {
  def read(reader: Reader[IO.Error]): IO[IO.Error, Option[T]]
}

object MapEntryReader {
  def read[T <: MapEntry[_, _]](bytes: Slice[Byte])(implicit serializer: MapEntryReader[T]): IO[IO.Error, Option[T]] =
    serializer.read(Reader(bytes))

  def read[T <: MapEntry[_, _]](reader: Reader[IO.Error])(implicit serializer: MapEntryReader[T]): IO[IO.Error, Option[T]] =
    serializer.read(reader)
}
