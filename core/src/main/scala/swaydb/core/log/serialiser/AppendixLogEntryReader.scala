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

package swaydb.core.log.serialiser

import swaydb.core.function.FunctionStore
import swaydb.core.file.ForceSaveApplier
import swaydb.core.log.LogEntry
import swaydb.core.segment.io.SegmentReadIO
import swaydb.core.segment.{Segment, SegmentSerialiser}
import swaydb.core.file.sweeper.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.file.sweeper.FileSweeper
import swaydb.core.segment.cache.sweeper.MemorySweeper
import swaydb.config.{MMAP, SegmentRefCacheLife}
import swaydb.slice.order.{KeyOrder, TimeOrder}
import swaydb.slice.{ReaderBase, Slice}

private[core] object AppendixLogEntryReader {
  def apply(mmapSegment: MMAP.Segment,
            segmentRefCacheLife: SegmentRefCacheLife)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                      timeOrder: TimeOrder[Slice[Byte]],
                                                      functionStore: FunctionStore,
                                                      keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                      fileSweeper: FileSweeper,
                                                      bufferCleaner: ByteBufferSweeperActor,
                                                      blockCacheSweeper: Option[MemorySweeper.Block],
                                                      forceSaveApplier: ForceSaveApplier,
                                                      segmentIO: SegmentReadIO): AppendixLogEntryReader =
    new AppendixLogEntryReader(
      mmapSegment = mmapSegment,
      segmentRefCacheLife = segmentRefCacheLife
    )
}

private[core] class AppendixLogEntryReader(mmapSegment: MMAP.Segment,
                                           segmentRefCacheLife: SegmentRefCacheLife)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                                     timeOrder: TimeOrder[Slice[Byte]],
                                                                                     functionStore: FunctionStore,
                                                                                     keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                                                     fileSweeper: FileSweeper,
                                                                                     bufferCleaner: ByteBufferSweeperActor,
                                                                                     blockCacheSweeper: Option[MemorySweeper.Block],
                                                                                     forceSaveApplier: ForceSaveApplier,
                                                                                     segmentIO: SegmentReadIO) {

  implicit object AppendixPutReader extends LogEntryReader[LogEntry.Put[Slice[Byte], Segment]] {
    override def read(reader: ReaderBase[Byte]): LogEntry.Put[Slice[Byte], Segment] = {
      val segment =
        SegmentSerialiser.FormatA.read(
          reader = reader,
          mmapSegment = mmapSegment,
          segmentRefCacheLife = segmentRefCacheLife,
          checkExists = false
        )

      LogEntry.Put(
        key = segment.minKey,
        value = segment
      )(AppendixLogEntryWriter.AppendixPutWriter)
    }
  }

  implicit object AppendixRemoveReader extends LogEntryReader[LogEntry.Remove[Slice[Byte]]] {
    override def read(reader: ReaderBase[Byte]): LogEntry.Remove[Slice[Byte]] = {
      val minKeyLength = reader.readUnsignedInt()
      val minKey: Slice[Byte] = reader.read(minKeyLength).cut()
      LogEntry.Remove(minKey)(AppendixLogEntryWriter.AppendixRemoveWriter)
    }
  }

  implicit object AppendixReader extends LogEntryReader[LogEntry[Slice[Byte], Segment]] {
    override def read(reader: ReaderBase[Byte]): LogEntry[Slice[Byte], Segment] =
      reader.foldLeft(null: LogEntry[Slice[Byte], Segment]) {
        case (previousEntryOrNull, reader) =>
          val entryId = reader.readUnsignedInt()
          if (entryId == AppendixLogEntryWriter.AppendixPutWriter.id) {
            val nextEntry = AppendixPutReader.read(reader)
            if (previousEntryOrNull == null)
              nextEntry
            else
              previousEntryOrNull ++ nextEntry
          } else if (entryId == AppendixLogEntryWriter.AppendixRemoveWriter.id) {
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
