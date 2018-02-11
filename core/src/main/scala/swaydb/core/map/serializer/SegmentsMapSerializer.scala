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
import java.nio.file.Paths

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.data.PersistentReadOnly
import swaydb.core.io.file.DBFile
import swaydb.core.map.MapEntry
import swaydb.core.map.MapEntry.{Add, Remove}
import swaydb.core.segment.Segment
import swaydb.core.util.ByteUtilCore
import swaydb.data.slice.{Reader, Slice}
import swaydb.data.util.ByteSizeOf

import scala.concurrent.ExecutionContext
import scala.util.{Success, Try}

private[core] object SegmentsMapSerializer {

  def apply(removeDeletedRecords: Boolean,
            mmapSegmentsOnRead: Boolean,
            mmapSegmentsOnWrite: Boolean,
            cacheKeysOnCreate: Boolean)(implicit ordering: Ordering[Slice[Byte]],
                                        keyValueLimiter: (PersistentReadOnly, Segment) => Unit,
                                        fileOpenLimited: DBFile => Unit,
                                        ec: ExecutionContext) =
    new SegmentsMapSerializer(removeDeletedRecords, mmapSegmentsOnRead, mmapSegmentsOnWrite, cacheKeysOnCreate)

}

private[core] class SegmentsMapSerializer(removeDeletes: Boolean,
                                          mmapSegmentsOnRead: Boolean,
                                          mmapSegmentsOnWrite: Boolean,
                                          cacheKeysOnCreate: Boolean)(implicit ordering: Ordering[Slice[Byte]],
                                                                      keyValueLimiter: (PersistentReadOnly, Segment) => Unit,
                                                                      fileOpenLimiter: DBFile => Unit,
                                                                      ec: ExecutionContext) extends MapSerializer[Slice[Byte], Segment] with LazyLogging {

  implicit val readerWriter = this

  override def read(reader: Reader): Try[Option[MapEntry[Slice[Byte], Segment]]] =
    reader.foldLeftTry(Option.empty[MapEntry[Slice[Byte], Segment]]) {
      case (mapEntries, reader) =>
        reader.get() flatMap {
          entryType =>
            if (entryType == 0) {
              for {
                minKeyLength <- reader.readIntUnsigned()
                minKey <- reader.read(minKeyLength).map(_.unslice())
              } yield {
                val entry = MapEntry.Remove(minKey)
                mapEntries.map(_ ++ entry) orElse Some(entry)
              }
            } else {
              for {
                segmentPathLength <- reader.readIntUnsigned()
                segmentPathBytes <- reader.read(segmentPathLength).map(_.unslice())
                segmentPath <- Success(Paths.get(new String(segmentPathBytes.toArray, StandardCharsets.UTF_8)))
                segmentSize <- reader.readIntUnsigned()
                minKeyLength <- reader.readIntUnsigned()
                minKey <- reader.read(minKeyLength).map(_.unslice())
                maxKeyLength <- reader.readIntUnsigned()
                maxKey <- reader.read(maxKeyLength).map(_.unslice())
                segment <- Segment(segmentPath, cacheKeysOnCreate, mmapSegmentsOnRead, mmapSegmentsOnWrite, minKey, maxKey, segmentSize, removeDeletes, checkExists = false)
              } yield {
                val entry = MapEntry.Add(minKey, segment)
                mapEntries.map(_ ++ entry) orElse Some(entry)
              }
            }
        }
    }

  override def writeTo(slice: Slice[Byte], entry: MapEntry[Slice[Byte], Segment]): Unit =
    entry match {
      case Add(key, segment) =>
        val segmentPath = Slice(segment.path.toString.getBytes(StandardCharsets.UTF_8))
        slice
          .addByte(1)
          .addIntUnsigned(segmentPath.size)
          .addBytes(segmentPath)
          .addIntUnsigned(segment.segmentSize)
          .addIntUnsigned(key.size)
          .addAll(key)
          .addIntUnsigned(segment.maxKey.size)
          .addAll(segment.maxKey)

      case Remove(key) =>
        slice
          .addByte(0)
          .addIntUnsigned(key.size)
          .addAll(key)
    }

  override def bytesRequiredFor(entry: MapEntry[Slice[Byte], Segment]): Int =
    entry match {
      case Add(key, segment) =>
        val segmentPath = segment.path.toString.getBytes(StandardCharsets.UTF_8)

        ByteSizeOf.byte +
          ByteUtilCore.sizeUnsignedInt(segmentPath.length) +
          segmentPath.length +
          ByteUtilCore.sizeUnsignedInt(segment.segmentSize) +
          ByteUtilCore.sizeUnsignedInt(key.size) +
          key.size +
          ByteUtilCore.sizeUnsignedInt(segment.maxKey.size) +
          segment.maxKey.size

      case Remove(key) =>
        ByteSizeOf.byte +
          ByteUtilCore.sizeUnsignedInt(key.size) +
          key.size
    }

}