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
import java.nio.file.{Path, Paths}
import java.util.concurrent.TimeUnit

import swaydb.Error.Map.ExceptionHandler
import swaydb.core.actor.{FileSweeper, MemorySweeper}
import swaydb.core.function.FunctionStore
import swaydb.core.io.file.{BlockCache, Effect}
import swaydb.core.map.MapEntry
import swaydb.core.segment.Segment
import swaydb.core.segment.format.a.block.SegmentIO
import swaydb.core.util.{BlockCacheFileIDGenerator, Bytes, Extension, MinMax}
import swaydb.data.MaxKey
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.{ReaderBase, Slice}
import swaydb.{Error, IO}

import scala.concurrent.duration.Deadline

object AppendixMapEntryReader {
  def apply(mmapSegmentsOnRead: Boolean,
            mmapSegmentsOnWrite: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                          timeOrder: TimeOrder[Slice[Byte]],
                                          functionStore: FunctionStore,
                                          keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                          fileSweeper: FileSweeper.Enabled,
                                          blockCache: Option[BlockCache.State],
                                          segmentIO: SegmentIO): AppendixMapEntryReader =
    new AppendixMapEntryReader(
      mmapSegmentsOnRead = mmapSegmentsOnRead,
      mmapSegmentsOnWrite = mmapSegmentsOnWrite
    )
}

case class AppendixSegment(path: Path,
                           mmapReads: Boolean,
                           mmapWrites: Boolean,
                           minKey: Slice[Byte],
                           maxKey: MaxKey[Slice[Byte]],
                           segmentSize: Int,
                           minMaxFunctionId: Option[MinMax[Slice[Byte]]],
                           nearestExpiryDeadline: Option[Deadline])

class AppendixMapEntryReader(mmapSegmentsOnRead: Boolean,
                             mmapSegmentsOnWrite: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                           timeOrder: TimeOrder[Slice[Byte]],
                                                           functionStore: FunctionStore,
                                                           keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                           fileSweeper: FileSweeper.Enabled,
                                                           blockCache: Option[BlockCache.State],
                                                           segmentIO: SegmentIO) {

  implicit object AppendixPutReader extends MapEntryReader[MapEntry.Put[Slice[Byte], Segment]] {
    override def read(reader: ReaderBase): IO[swaydb.Error.Map, Option[MapEntry.Put[Slice[Byte], Segment]]] =
      IO {
        val segmentPathLength = reader.readUnsignedInt()
        val segmentPathBytes = reader.read(segmentPathLength).unslice()
        val segmentPath = Paths.get(new String(segmentPathBytes.toArray, StandardCharsets.UTF_8))
        val segmentSize = reader.readUnsignedInt()
        val minKeyLength = reader.readUnsignedInt()
        val minKey = reader.read(minKeyLength).unslice()
        val maxKeyId = reader.readUnsignedInt()
        val maxKeyLength = reader.readUnsignedInt()
        val maxKeyBytes = reader.read(maxKeyLength).unslice()

        val maxKey =
          if (maxKeyId == 1) {
            MaxKey.Fixed(maxKeyBytes)
          } else {
            val (fromKey, toKey) = Bytes.decompressJoin(maxKeyBytes)
            MaxKey.Range(fromKey, toKey)
          }

        val nearestExpiryDeadline = {
          val deadlineNanos = reader.readUnsignedLong()
          if (deadlineNanos == 0)
            None
          else
            Some(Deadline(deadlineNanos, TimeUnit.NANOSECONDS))
        }

        val minMaxFunctionId = {
          val minIdSize = reader.readUnsignedInt()
          if (minIdSize == 0)
            None
          else {
            val minId = reader.read(minIdSize)
            val maxIdSize = reader.readUnsignedInt()
            val maxId = if (maxIdSize == 0) None else Some(reader.read(maxIdSize))
            Some(MinMax(minId, maxId))
          }
        }

        val segmentResult =
          IO(Effect.fileId(segmentPath)) flatMap {
            case (segmentId, Extension.Seg) =>

              /**
               * Not a very nice way of converting [[Segment]] initialisation errors into [[swaydb.Error.Segment]]
               * as Map readers only know about [[swaydb.Error.Map]].
               *
               * If an error is Segment specific it gets wrapped as [[swaydb.Error.Fatal]] which get reopened
               * and converted back to [[swaydb.Error.Level]] when the Level initialises.
               *
               * A needs to be a design update on how MapReader read data. MapReader should simply read the data
               * parse it to a type and without knowing about [[Segment]] or any other data type this way Map can
               * initialise and handle errors within themselves without having to know about the outside.
               */

              IO {
                Segment(
                  path = segmentPath,
                  segmentId = segmentId,
                  mmapReads = mmapSegmentsOnRead,
                  mmapWrites = mmapSegmentsOnWrite,
                  blockCacheFileId = BlockCacheFileIDGenerator.nextID,
                  minKey = minKey,
                  maxKey = maxKey,
                  segmentSize = segmentSize,
                  minMaxFunctionId = minMaxFunctionId,
                  nearestExpiryDeadline = nearestExpiryDeadline,
                  checkExists = false
                )
              } match {
                case IO.Right(segment) =>
                  IO.Right {
                    Some(
                      MapEntry.Put(
                        key = minKey,
                        value = segment
                      )(AppendixMapEntryWriter.AppendixPutWriter)
                    )
                  }

                case IO.Left(error) =>
                  error match {
                    case Error.Fatal(exception) =>
                      IO.Left(Error.Fatal(exception))

                    case io: Error.IO =>
                      IO.Left(io)

                    case other: swaydb.Error.Segment =>
                      //convert Segment error to fatal.
                      IO.Left(Error.Fatal(other.exception))
                  }
              }

            case (segmentId, Extension.Log) =>
              IO.failed(s"Invalid segment extension: $segmentPath")
          }

        segmentResult.get
      }
  }

  implicit object AppendixRemoveReader extends MapEntryReader[MapEntry.Remove[Slice[Byte]]] {
    override def read(reader: ReaderBase): IO[swaydb.Error.Map, Option[MapEntry.Remove[Slice[Byte]]]] =
      IO {
        val minKeyLength = reader.readUnsignedInt()
        val minKey = reader.read(minKeyLength).unslice()
        Some(MapEntry.Remove(minKey)(AppendixMapEntryWriter.AppendixRemoveWriter))
      }
  }

  implicit object AppendixReader extends MapEntryReader[MapEntry[Slice[Byte], Segment]] {
    override def read(reader: ReaderBase): IO[swaydb.Error.Map, Option[MapEntry[Slice[Byte], Segment]]] =
      reader.foldLeftIO(Option.empty[MapEntry[Slice[Byte], Segment]]) {
        case (previousEntry, reader) =>
          IO(reader.readUnsignedInt()) flatMap {
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
                IO.failed(new IllegalArgumentException(s"Invalid entry type $entryId."))
          }
      }
  }
}
