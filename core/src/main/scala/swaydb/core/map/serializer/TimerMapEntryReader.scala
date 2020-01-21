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
 */

package swaydb.core.map.serializer

import swaydb.core.map.MapEntry
import swaydb.data.slice.{ReaderBase, Slice}

private[core] object TimerMapEntryReader {

  implicit object TimerPutMapEntryReader extends MapEntryReader[MapEntry[Slice[Byte], Slice[Byte]]] {
    override def read(reader: ReaderBase): MapEntry.Put[Slice[Byte], Slice[Byte]] = {
      val _ = reader.readUnsignedInt()
      val keySize = reader.readUnsignedInt()

      val key =
        if (keySize == 0)
          Slice.emptyBytes
        else
          reader.read(keySize)

      val valueSize = reader.readUnsignedInt()

      val value =
        if (valueSize == 0)
          Slice.emptyBytes
        else
          reader.read(valueSize)

      MapEntry.Put(key, value)(TimerMapEntryWriter.TimerPutMapEntryWriter)
    }
  }
}
