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

package swaydb.core.map.serializer

import swaydb.core.map.MapEntry
import swaydb.data.slice.{ReaderBase, Slice}

private[swaydb] object CounterMapEntryReader {

  implicit object CounterPutMapEntryReader extends MapEntryReader[MapEntry[Slice[Byte], Slice[Byte]]] {
    override def read(reader: ReaderBase[Byte]): MapEntry.Put[Slice[Byte], Slice[Byte]] = {
      val _ = reader.get()
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

      MapEntry.Put(key, value)(CounterMapEntryWriter.CounterPutMapEntryWriter)
    }
  }
}
