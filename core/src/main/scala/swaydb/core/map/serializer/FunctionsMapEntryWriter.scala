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
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.core.map.serializer

import swaydb.core.map.MapEntry
import swaydb.data.slice.Slice
import swaydb.data.slice.Slice.Slice
import swaydb.data.util.ByteSizeOf

private[swaydb] object FunctionsMapEntryWriter {

  implicit object FunctionsPutMapEntryWriter extends MapEntryWriter[MapEntry.Put[Slice[Byte], Slice.Null.type]] {
    val id: Byte = 0

    override val isRange: Boolean = false
    override val isUpdate: Boolean = false

    override def write(entry: MapEntry.Put[Slice[Byte], Slice.Null.type], bytes: Slice[Byte]): Unit =
      bytes
        .add(id)
        .addAll(entry.key)

    override def bytesRequired(entry: MapEntry.Put[Slice[Byte], Slice.Null.type]): Int =
      ByteSizeOf.byte +
        entry.key.size
  }

  implicit object FunctionsRemoveMapEntryWriter extends MapEntryWriter[MapEntry.Remove[Slice[Byte]]] {
    val id: Byte = 1

    override val isRange: Boolean = false
    override val isUpdate: Boolean = false

    override def write(entry: MapEntry.Remove[Slice[Byte]], bytes: Slice[Byte]): Unit =
      bytes
        .add(id)
        .addAll(entry.key)

    override def bytesRequired(entry: MapEntry.Remove[Slice[Byte]]): Int =
      ByteSizeOf.byte +
        entry.key.size
  }
}
