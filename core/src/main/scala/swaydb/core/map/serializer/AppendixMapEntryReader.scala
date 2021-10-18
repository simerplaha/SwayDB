/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb.core.map.serializer

import swaydb.core.function.FunctionStore
import swaydb.core.io.file.ForceSaveApplier
import swaydb.core.map.MapEntry
import swaydb.core.segment.io.SegmentReadIO
import swaydb.core.segment.{Segment, SegmentSerialiser}
import swaydb.core.sweeper.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.sweeper.{FileSweeper, MemorySweeper}
import swaydb.data.config.{MMAP, SegmentRefCacheLife}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.{ReaderBase, Slice}

private[core] object AppendixMapEntryReader {
  def apply(mmapSegment: MMAP.Segment,
            segmentRefCacheLife: SegmentRefCacheLife)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                      timeOrder: TimeOrder[Slice[Byte]],
                                                      functionStore: FunctionStore,
                                                      keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                      fileSweeper: FileSweeper,
                                                      bufferCleaner: ByteBufferSweeperActor,
                                                      blockCacheSweeper: Option[MemorySweeper.Block],
                                                      forceSaveApplier: ForceSaveApplier,
                                                      segmentIO: SegmentReadIO): AppendixMapEntryReader =
    new AppendixMapEntryReader(
      mmapSegment = mmapSegment,
      segmentRefCacheLife = segmentRefCacheLife
    )
}

private[core] class AppendixMapEntryReader(mmapSegment: MMAP.Segment,
                                           segmentRefCacheLife: SegmentRefCacheLife)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                                     timeOrder: TimeOrder[Slice[Byte]],
                                                                                     functionStore: FunctionStore,
                                                                                     keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                                                     fileSweeper: FileSweeper,
                                                                                     bufferCleaner: ByteBufferSweeperActor,
                                                                                     blockCacheSweeper: Option[MemorySweeper.Block],
                                                                                     forceSaveApplier: ForceSaveApplier,
                                                                                     segmentIO: SegmentReadIO) {

  implicit object AppendixPutReader extends MapEntryReader[MapEntry.Put[Slice[Byte], Segment]] {
    override def read(reader: ReaderBase[Byte]): MapEntry.Put[Slice[Byte], Segment] = {
      val segment =
        SegmentSerialiser.FormatA.read(
          reader = reader,
          mmapSegment = mmapSegment,
          segmentRefCacheLife = segmentRefCacheLife,
          checkExists = false
        )

      MapEntry.Put(
        key = segment.minKey,
        value = segment
      )(AppendixMapEntryWriter.AppendixPutWriter)
    }
  }

  implicit object AppendixRemoveReader extends MapEntryReader[MapEntry.Remove[Slice[Byte]]] {
    override def read(reader: ReaderBase[Byte]): MapEntry.Remove[Slice[Byte]] = {
      val minKeyLength = reader.readUnsignedInt()
      val minKey = reader.read(minKeyLength).unslice()
      MapEntry.Remove(minKey)(AppendixMapEntryWriter.AppendixRemoveWriter)
    }
  }

  implicit object AppendixReader extends MapEntryReader[MapEntry[Slice[Byte], Segment]] {
    override def read(reader: ReaderBase[Byte]): MapEntry[Slice[Byte], Segment] =
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
