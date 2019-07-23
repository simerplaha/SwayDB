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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.map.serializer

import java.nio.charset.StandardCharsets
import java.nio.file.Paths
import java.util.concurrent.TimeUnit

import swaydb.IO
import swaydb.core.function.FunctionStore
import swaydb.core.group.compression.data.KeyValueGroupingStrategyInternal
import swaydb.core.map.MapEntry
import swaydb.core.queue.{FileLimiter, KeyValueLimiter}
import swaydb.core.segment.Segment
import swaydb.core.segment.format.a.block.SegmentIO
import swaydb.core.util.{Bytes, MinMax}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.{Reader, Slice}
import swaydb.data.MaxKey
import swaydb.ErrorHandler.CoreErrorHandler

import scala.concurrent.duration.Deadline

object AppendixMapEntryReader {
  def apply(mmapSegmentsOnRead: Boolean,
            mmapSegmentsOnWrite: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                          timeOrder: TimeOrder[Slice[Byte]],
                                          functionStore: FunctionStore,
                                          keyValueLimiter: KeyValueLimiter,
                                          fileOpenLimiter: FileLimiter,
                                          segmentIO: SegmentIO,
                                          compression: Option[KeyValueGroupingStrategyInternal]): AppendixMapEntryReader =
    new AppendixMapEntryReader(
      mmapSegmentsOnRead = mmapSegmentsOnRead,
      mmapSegmentsOnWrite = mmapSegmentsOnWrite
    )
}

class AppendixMapEntryReader(mmapSegmentsOnRead: Boolean,
                             mmapSegmentsOnWrite: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                           timeOrder: TimeOrder[Slice[Byte]],
                                                           functionStore: FunctionStore,
                                                           keyValueLimiter: KeyValueLimiter,
                                                           fileOpenLimiter: FileLimiter,
                                                           segmentIO: SegmentIO,
                                                           compression: Option[KeyValueGroupingStrategyInternal]) {

  implicit object AppendixPutReader extends MapEntryReader[MapEntry.Put[Slice[Byte], Segment]] {
    override def read(reader: Reader[IO.Error]): IO[IO.Error, Option[MapEntry.Put[Slice[Byte], Segment]]] =
      for {
        segmentPathLength <- reader.readIntUnsigned()
        segmentPathBytes <- reader.read(segmentPathLength).map(_.unslice())
        segmentPath <- IO(Paths.get(new String(segmentPathBytes.toArray, StandardCharsets.UTF_8)))
        segmentSize <- reader.readIntUnsigned()
        minKeyLength <- reader.readIntUnsigned()
        minKey <- reader.read(minKeyLength).map(_.unslice())
        maxKeyId <- reader.readIntUnsigned()
        maxKeyLength <- reader.readIntUnsigned()
        maxKeyBytes <- reader.read(maxKeyLength).map(_.unslice())
        maxKey <- {
          if (maxKeyId == 1)
            IO.Success(MaxKey.Fixed(maxKeyBytes))
          else {
            Bytes.decompressJoin(maxKeyBytes) map {
              case (fromKey, toKey) =>
                MaxKey.Range(fromKey, toKey)
            }
          }
        }
        nearestExpiryDeadline <- {
          reader.readLongUnsigned() map {
            deadlineNanos =>
              if (deadlineNanos == 0)
                None
              else
                Some(Deadline(deadlineNanos, TimeUnit.NANOSECONDS))
          }
        }
        minMaxFunctionId <- {
          reader.readIntUnsigned() flatMap {
            minIdSize =>
              if (minIdSize == 0)
                IO.none
              else
                for {
                  minId <- reader.read(minIdSize)
                  maxIdSize <- reader.readIntUnsigned()
                  maxId <- if (maxIdSize == 0) IO.none else reader.read(maxIdSize).map(Some(_))
                } yield Some(MinMax(minId, maxId))
          }
        }
        segment <- {
          Segment(
            path = segmentPath,
            mmapReads = mmapSegmentsOnRead,
            mmapWrites = mmapSegmentsOnWrite,
            minKey = minKey,
            maxKey = maxKey,
            segmentSize = segmentSize,
            minMaxFunctionId = minMaxFunctionId,
            nearestExpiryDeadline = nearestExpiryDeadline,
            checkExists = false
          )
        }
      } yield {
        Some(
          MapEntry.Put(
            key = minKey,
            value = segment
          )(AppendixMapEntryWriter.AppendixPutWriter)
        )
      }
  }

  implicit object AppendixRemoveReader extends MapEntryReader[MapEntry.Remove[Slice[Byte]]] {
    override def read(reader: Reader[IO.Error]): IO[IO.Error, Option[MapEntry.Remove[Slice[Byte]]]] =
      for {
        minKeyLength <- reader.readIntUnsigned()
        minKey <- reader.read(minKeyLength).map(_.unslice())
      } yield {
        Some(MapEntry.Remove(minKey)(AppendixMapEntryWriter.AppendixRemoveWriter))
      }
  }

  implicit object AppendixReader extends MapEntryReader[MapEntry[Slice[Byte], Segment]] {
    override def read(reader: Reader[IO.Error]): IO[IO.Error, Option[MapEntry[Slice[Byte], Segment]]] =
      reader.foldLeftIO(Option.empty[MapEntry[Slice[Byte], Segment]]) {
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
                IO.Failure(new IllegalArgumentException(s"Invalid entry type $entryId."))
          }
      }
  }
}
