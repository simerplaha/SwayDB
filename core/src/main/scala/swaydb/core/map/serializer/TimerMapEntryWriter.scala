/*
 * Copyright (c) 2020 Simer Plaha (@simerplaha)
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
import swaydb.core.util.Bytes
import swaydb.data.slice.Slice

private[core] object TimerMapEntryWriter {

  implicit object TimerPutMapEntryWriter extends MapEntryWriter[MapEntry.Put[Slice[Byte], Slice[Byte]]] {
    val id: Int = 0

    override val isRange: Boolean = false
    override val isUpdate: Boolean = false

    override def write(entry: MapEntry.Put[Slice[Byte], Slice[Byte]], bytes: Slice[Byte]): Unit =
      bytes
        .addUnsignedInt(id)
        .addUnsignedInt(entry.key.size)
        .addAll(entry.key)
        .addUnsignedInt(entry.value.size)
        .addAll(entry.value)

    override def bytesRequired(entry: MapEntry.Put[Slice[Byte], Slice[Byte]]): Int =
      Bytes.sizeOfUnsignedInt(id) +
        Bytes.sizeOfUnsignedInt(entry.key.size) +
        entry.key.size +
        Bytes.sizeOfUnsignedInt(entry.value.size) +
        entry.value.size
  }
}
