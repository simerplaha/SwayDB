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

import java.nio.charset.StandardCharsets

import swaydb.core.map.MapEntry
import swaydb.core.segment.Segment
import swaydb.core.util.Bytes
import swaydb.data.MaxKey
import swaydb.data.slice.Slice
import swaydb.core.util.Options._

object AppendixMapEntryWriter {

  implicit object AppendixRemoveWriter extends MapEntryWriter[MapEntry.Remove[Slice[Byte]]] {
    val id: Int = 0

    override val isRange: Boolean = false
    override val isUpdate: Boolean = false

    override def write(entry: MapEntry.Remove[Slice[Byte]], bytes: Slice[Byte]): Unit =
      bytes
        .addUnsignedInt(id)
        .addUnsignedInt(entry.key.size)
        .addAll(entry.key)

    override def bytesRequired(entry: MapEntry.Remove[Slice[Byte]]): Int =
      Bytes.sizeOfUnsignedInt(id) +
        Bytes.sizeOfUnsignedInt(entry.key.size) +
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

      def writeMinMax(bytes: Slice[Byte]) =
        entry.value.minMaxFunctionId match {
          case Some(minMaxFunctionId) =>
            bytes addUnsignedInt minMaxFunctionId.min.size
            bytes addAll minMaxFunctionId.min
            minMaxFunctionId.max match {
              case Some(max) =>
                bytes addUnsignedInt max.size
                bytes addAll max

              case None =>
                bytes addUnsignedInt 0
            }

          case None =>
            bytes addUnsignedInt 0
        }

      bytes
        .addUnsignedInt(id)
        .addUnsignedInt(segmentPath.size)
        .addBytes(segmentPath)
        .addUnsignedInt(entry.value.createdInLevel)
        .addUnsignedInt(entry.value.segmentSize)
        .addUnsignedInt(entry.key.size)
        .addAll(entry.key)
        .addUnsignedInt(maxKeyId)
        .addUnsignedInt(maxKeyBytes.size)
        .addAll(maxKeyBytes)
        .addUnsignedLong(entry.value.nearestPutDeadline.valueOrElse(_.time.toNanos, 0L))

      writeMinMax(bytes)
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

      val minMaxFunctionIdBytesRequires =
        entry.value.minMaxFunctionId match {
          case Some(minMax) =>
            Bytes.sizeOfUnsignedInt(minMax.min.size) +
              minMax.min.size +
              Bytes.sizeOfUnsignedInt(minMax.max.valueOrElse(_.size, 0)) +
              minMax.max.valueOrElse(_.size, 0)

          case None =>
            1
        }

      Bytes.sizeOfUnsignedInt(id) +
        Bytes.sizeOfUnsignedInt(segmentPath.length) +
        segmentPath.length +
        Bytes.sizeOfUnsignedInt(entry.value.createdInLevel) +
        Bytes.sizeOfUnsignedInt(entry.value.segmentSize) +
        Bytes.sizeOfUnsignedInt(entry.key.size) +
        entry.key.size +
        Bytes.sizeOfUnsignedInt(maxKeyId) +
        Bytes.sizeOfUnsignedInt(maxKeyBytes.size) +
        maxKeyBytes.size +
        Bytes.sizeOfUnsignedLong(entry.value.nearestPutDeadline.valueOrElse(_.time.toNanos, 0L)) +
        minMaxFunctionIdBytesRequires
    }
  }

}
