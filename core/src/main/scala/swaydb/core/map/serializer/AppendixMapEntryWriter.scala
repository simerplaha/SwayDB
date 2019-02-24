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

import java.nio.charset.StandardCharsets
import swaydb.core.map.MapEntry
import swaydb.core.segment.Segment
import swaydb.core.util.Bytes
import swaydb.data.MaxKey
import swaydb.data.slice.Slice

object AppendixMapEntryWriter {

  implicit object AppendixRemoveWriter extends MapEntryWriter[MapEntry.Remove[Slice[Byte]]] {
    val id: Int = 0

    override val isRange: Boolean = false
    override val isUpdate: Boolean = false

    override def write(entry: MapEntry.Remove[Slice[Byte]], bytes: Slice[Byte]): Unit =
      bytes
        .addIntUnsigned(id)
        .addIntUnsigned(entry.key.size)
        .addAll(entry.key)

    override def bytesRequired(entry: MapEntry.Remove[Slice[Byte]]): Int =
      Bytes.sizeOf(id) +
        Bytes.sizeOf(entry.key.size) +
        entry.key.size
  }

  implicit object AppendixPutWriter extends MapEntryWriter[MapEntry.Put[Slice[Byte], Segment]] {
    val id: Int = 1

    override val isRange: Boolean = false
    override val isUpdate: Boolean = false

    override def write(entry: MapEntry.Put[Slice[Byte], Segment], bytes: Slice[Byte]): Unit = {
      val segmentPath = Slice(entry.value.path.toString.getBytes(StandardCharsets.UTF_8))
      val (maxKeyId, maxKeyBytes) =
        entry.value.maxKey match {
          case MaxKey.Fixed(maxKey) =>
            (1, maxKey)
          case MaxKey.Range(fromKey, maxToKey) =>
            (2, Bytes.compressJoin(fromKey, maxToKey))
        }
      bytes
        .addIntUnsigned(id)
        .addIntUnsigned(segmentPath.size)
        .addBytes(segmentPath)
        .addIntUnsigned(entry.value.segmentSize)
        .addIntUnsigned(entry.key.size)
        .addAll(entry.key)
        .addIntUnsigned(maxKeyId)
        .addIntUnsigned(maxKeyBytes.size)
        .addAll(maxKeyBytes)
        .addLongUnsigned(entry.value.nearestExpiryDeadline.map(_.time.toNanos).getOrElse(0L))
    }

    override def bytesRequired(entry: MapEntry.Put[Slice[Byte], Segment]): Int = {
      val segmentPath = entry.value.path.toString.getBytes(StandardCharsets.UTF_8)

      val (maxKeyId, maxKeyBytes) =
        entry.value.maxKey match {
          case MaxKey.Fixed(maxKey) =>
            (1, maxKey)
          case MaxKey.Range(fromKey, maxToKey) =>
            (2, Bytes.compressJoin(fromKey, maxToKey))
        }

      Bytes.sizeOf(id) +
        Bytes.sizeOf(segmentPath.length) +
        segmentPath.length +
        Bytes.sizeOf(entry.value.segmentSize) +
        Bytes.sizeOf(entry.key.size) +
        entry.key.size +
        Bytes.sizeOf(maxKeyId) +
        Bytes.sizeOf(maxKeyBytes.size) +
        maxKeyBytes.size +
        Bytes.sizeOf(entry.value.nearestExpiryDeadline.map(_.time.toNanos).getOrElse(0L))
    }
  }

}
