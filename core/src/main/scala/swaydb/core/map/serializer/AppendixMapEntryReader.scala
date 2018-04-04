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

import swaydb.core.data.Persistent
import swaydb.core.io.file.DBFile
import swaydb.core.map.MapEntry
import swaydb.core.segment.Segment
import swaydb.data.segment.MaxKey
import swaydb.core.util.ByteUtilCore
import swaydb.data.slice.{Reader, Slice}

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}

object AppendixMapEntryReader {
  def apply(removeDeletes: Boolean,
            mmapSegmentsOnRead: Boolean,
            mmapSegmentsOnWrite: Boolean,
            cacheKeysOnCreate: Boolean)(implicit ordering: Ordering[Slice[Byte]],
                                        keyValueLimiter: (Persistent, Segment) => Unit,
                                        fileOpenLimiter: DBFile => Unit,
                                        ec: ExecutionContext): AppendixMapEntryReader =
    new AppendixMapEntryReader(
      removeDeletes = removeDeletes,
      mmapSegmentsOnRead = mmapSegmentsOnRead,
      mmapSegmentsOnWrite = mmapSegmentsOnWrite,
      cacheKeysOnCreate = cacheKeysOnCreate
    )
}

class AppendixMapEntryReader(removeDeletes: Boolean,
                             mmapSegmentsOnRead: Boolean,
                             mmapSegmentsOnWrite: Boolean,
                             cacheKeysOnCreate: Boolean)(implicit ordering: Ordering[Slice[Byte]],
                                                         keyValueLimiter: (Persistent, Segment) => Unit,
                                                         fileOpenLimiter: DBFile => Unit,
                                                         ec: ExecutionContext) {

  implicit object AppendixPutReader extends MapEntryReader[MapEntry.Put[Slice[Byte], Segment]] {
    override def read(reader: Reader): Try[Option[MapEntry.Put[Slice[Byte], Segment]]] =
      for {
        segmentPathLength <- reader.readIntUnsigned()
        segmentPathBytes <- reader.read(segmentPathLength).map(_.unslice())
        segmentPath <- Try(Paths.get(new String(segmentPathBytes.toArray, StandardCharsets.UTF_8)))
        segmentSize <- reader.readIntUnsigned()
        minKeyLength <- reader.readIntUnsigned()
        minKey <- reader.read(minKeyLength).map(_.unslice())
        maxKeyId <- reader.readIntUnsigned()
        maxKeyLength <- reader.readIntUnsigned()
        maxKeyBytes <- reader.read(maxKeyLength).map(_.unslice())
        maxKey <-
          if (maxKeyId == 1)
            Success(MaxKey.Fixed(maxKeyBytes))
          else {
            ByteUtilCore.uncompress(maxKeyBytes) map {
              case (fromKey, toKey) =>
                MaxKey.Range(fromKey, toKey)
            }
          }
        segment <- Segment(segmentPath, cacheKeysOnCreate, mmapSegmentsOnRead, mmapSegmentsOnWrite, minKey, maxKey, segmentSize, removeDeletes, checkExists = false)
      } yield {
        Some(MapEntry.Put(minKey, segment)(AppendixMapEntryWriter.AppendixPutWriter))
      }
  }

  implicit object AppendixRemoveReader extends MapEntryReader[MapEntry.Remove[Slice[Byte]]] {
    override def read(reader: Reader): Try[Option[MapEntry.Remove[Slice[Byte]]]] =
      for {
        minKeyLength <- reader.readIntUnsigned()
        minKey <- reader.read(minKeyLength).map(_.unslice())
      } yield {
        Some(MapEntry.Remove(minKey)(AppendixMapEntryWriter.AppendixRemoveWriter))
      }
  }

  implicit object AppendixReader extends MapEntryReader[MapEntry[Slice[Byte], Segment]] {
    override def read(reader: Reader): Try[Option[MapEntry[Slice[Byte], Segment]]] =
      reader.foldLeftTry(Option.empty[MapEntry[Slice[Byte], Segment]]) {
        case (previousEntry, reader) =>
          reader.readIntUnsigned() flatMap {
            entryId =>
              if (entryId == AppendixMapEntryWriter.AppendixPutWriter.id)
                AppendixPutReader.read(reader) map {
                  nextEntry =>
                    nextEntry flatMap {
                      nextEntry =>
                        previousEntry.map(_ ++ nextEntry) orElse Some(nextEntry)
                    }
                }
              else if (entryId == AppendixMapEntryWriter.AppendixRemoveWriter.id)
                AppendixRemoveReader.read(reader) map {
                  nextEntry =>
                    nextEntry flatMap {
                      nextEntry =>
                        previousEntry.map(_ ++ nextEntry) orElse Some(nextEntry)
                    }
                }
              else
                Failure(new IllegalArgumentException(s"Invalid entry type $entryId."))
          }
      }
  }
}