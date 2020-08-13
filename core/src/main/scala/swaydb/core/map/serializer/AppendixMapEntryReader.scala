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

import swaydb.core.actor.{FileSweeper, MemorySweeper}
import swaydb.core.function.FunctionStore
import swaydb.core.actor.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.actor.FileSweeper.FileSweeperActor
import swaydb.core.io.file.BlockCache
import swaydb.core.map.MapEntry
import swaydb.core.segment.{Segment, SegmentIO, SegmentSerialiser}
import swaydb.data.config.MMAP
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.{ReaderBase, Slice}

private[core] object AppendixMapEntryReader {
  def apply(mmapSegment: MMAP.Segment)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                       timeOrder: TimeOrder[Slice[Byte]],
                                       functionStore: FunctionStore,
                                       keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                       fileSweeper: FileSweeperActor,
                                       bufferCleaner: ByteBufferSweeperActor,
                                       blockCache: Option[BlockCache.State],
                                       segmentIO: SegmentIO): AppendixMapEntryReader =
    new AppendixMapEntryReader(mmapSegment)
}

private[core] class AppendixMapEntryReader(mmapSegment: MMAP.Segment)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                      timeOrder: TimeOrder[Slice[Byte]],
                                                                      functionStore: FunctionStore,
                                                                      keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                                      fileSweeper: FileSweeperActor,
                                                                      bufferCleaner: ByteBufferSweeperActor,
                                                                      blockCache: Option[BlockCache.State],
                                                                      segmentIO: SegmentIO) {

  implicit object AppendixPutReader extends MapEntryReader[MapEntry.Put[Slice[Byte], Segment]] {
    override def read(reader: ReaderBase): MapEntry.Put[Slice[Byte], Segment] = {
      val segment =
        SegmentSerialiser.FormatA.read(
          reader = reader,
          mmapSegment = mmapSegment,
          checkExists = false
        )

      MapEntry.Put(
        key = segment.minKey,
        value = segment
      )(AppendixMapEntryWriter.AppendixPutWriter)
    }
  }

  implicit object AppendixRemoveReader extends MapEntryReader[MapEntry.Remove[Slice[Byte]]] {
    override def read(reader: ReaderBase): MapEntry.Remove[Slice[Byte]] = {
      val minKeyLength = reader.readUnsignedInt()
      val minKey = reader.read(minKeyLength).unslice()
      MapEntry.Remove(minKey)(AppendixMapEntryWriter.AppendixRemoveWriter)
    }
  }

  implicit object AppendixReader extends MapEntryReader[MapEntry[Slice[Byte], Segment]] {
    override def read(reader: ReaderBase): MapEntry[Slice[Byte], Segment] =
      reader.foldLeft(null: MapEntry[Slice[Byte], Segment]) {
        case (previousEntryOrNull, reader) =>
          val entryId = reader.readUnsignedInt()
          if (entryId == AppendixMapEntryWriter.AppendixPutWriter.id) {
            val nextEntry = AppendixPutReader.read(reader)
            if (previousEntryOrNull == null)
              nextEntry
            else
              previousEntryOrNull ++ nextEntry
          } else if (entryId == AppendixMapEntryWriter.AppendixRemoveWriter.id) {
            val nextEntry = AppendixRemoveReader.read(reader)
            if (previousEntryOrNull == null)
              nextEntry
            else
              previousEntryOrNull ++ nextEntry
          } else {
            throw new IllegalArgumentException(s"Invalid entry type $entryId.")
          }
      }
  }
}
