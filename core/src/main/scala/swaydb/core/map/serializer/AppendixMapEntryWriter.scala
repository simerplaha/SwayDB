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

package swaydb.core.map.serializer

import java.nio.charset.StandardCharsets

import swaydb.core.map.MapEntry
import swaydb.core.segment.Segment
import swaydb.core.util.ByteUtilCore
import swaydb.data.slice.Slice

object AppendixMapEntryWriter {

  implicit object AppendixRemoveWriter extends MapEntryWriter[MapEntry.Remove[Slice[Byte]]] {
    val id: Int = 0

    override def write(entry: MapEntry.Remove[Slice[Byte]], bytes: Slice[Byte]): Unit =
      bytes
        .addIntUnsigned(id)
        .addIntUnsigned(entry.key.size)
        .addAll(entry.key)

    override def bytesRequired(entry: MapEntry.Remove[Slice[Byte]]): Int =
      ByteUtilCore.sizeUnsignedInt(id) +
        ByteUtilCore.sizeUnsignedInt(entry.key.size) +
        entry.key.size

    override val isRange: Boolean = false
  }

  implicit object AppendixPutWriter extends MapEntryWriter[MapEntry.Put[Slice[Byte], Segment]] {
    val id: Int = 1

    override def write(entry: MapEntry.Put[Slice[Byte], Segment], bytes: Slice[Byte]): Unit = {
      val segmentPath = Slice(entry.value.path.toString.getBytes(StandardCharsets.UTF_8))
      bytes
        .addIntUnsigned(id)
        .addIntUnsigned(segmentPath.size)
        .addBytes(segmentPath)
        .addIntUnsigned(entry.value.segmentSize)
        .addIntUnsigned(entry.key.size)
        .addAll(entry.key)
        .addIntUnsigned(entry.value.maxKey.size)
        .addAll(entry.value.maxKey)
    }

    override def bytesRequired(entry: MapEntry.Put[Slice[Byte], Segment]): Int = {
      val segmentPath = entry.value.path.toString.getBytes(StandardCharsets.UTF_8)

      ByteUtilCore.sizeUnsignedInt(id) +
        ByteUtilCore.sizeUnsignedInt(segmentPath.length) +
        segmentPath.length +
        ByteUtilCore.sizeUnsignedInt(entry.value.segmentSize) +
        ByteUtilCore.sizeUnsignedInt(entry.key.size) +
        entry.key.size +
        ByteUtilCore.sizeUnsignedInt(entry.value.maxKey.size) +
        entry.value.maxKey.size

    }

    override val isRange: Boolean = false
  }

  implicit object AppendixMapEntry extends MapEntryWriter[MapEntry[Slice[Byte], Segment]] {

    override def write(entry: MapEntry[Slice[Byte], Segment], bytes: Slice[Byte]): Unit =
      entry.writeTo(bytes)

    override def bytesRequired(entry: MapEntry[Slice[Byte], Segment]): Int =
      entry.entryBytesSize

    override val isRange: Boolean = false
  }
}
