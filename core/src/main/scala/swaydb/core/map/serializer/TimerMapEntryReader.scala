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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.map.serializer

import swaydb.core.map.MapEntry
import swaydb.data.IO
import swaydb.data.slice.{Reader, Slice}

object TimerMapEntryReader {

  implicit object TimerPutMapEntryReader extends MapEntryReader[MapEntry[Slice[Byte], Slice[Byte]]] {
    override def read(reader: Reader): IO[Option[MapEntry.Put[Slice[Byte], Slice[Byte]]]] =
      for {
        id <- reader.readIntUnsigned()
        keySize <- reader.readIntUnsigned()
        key <-
          if (keySize == 0)
            IO.emptyBytes
          else
            reader.read(keySize)
        valueSize <- reader.readIntUnsigned()
        value <-
          if (valueSize == 0)
            IO.emptyBytes
          else
            reader.read(valueSize)
      } yield {
        Some(MapEntry.Put(key, value)(TimerMapEntryWriter.TimerPutMapEntryWriter))
      }
  }
}
