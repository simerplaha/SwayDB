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

package swaydb.core.segment.io

import swaydb.core.data.DefIO
import swaydb.core.function.FunctionStore
import swaydb.core.io.file.ForceSaveApplier
import swaydb.core.level.PathsDistributor
import swaydb.core.segment._
import swaydb.core.segment.block.segment.transient.TransientSegment
import swaydb.core.sweeper.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.sweeper.{FileSweeper, MemorySweeper}
import swaydb.core.util.IDGenerator
import swaydb.data.config.{MMAP, SegmentRefCacheLife}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.slice.Slice
import swaydb.{Error, IO}

/**
 * Provides implementation on how [[TransientSegment]]
 * should be persisted.
 *
 * Used by [[swaydb.core.level.compaction.Compaction]] to delay persisting
 * [[TransientSegment]] on dedicated IO ExecutionContext.
 *
 */
trait SegmentWriteIO[-T <: TransientSegment, S] {

  def minKey(segment: S): Slice[Byte]

  def persistMerged(pathsDistributor: PathsDistributor,
                    segmentRefCacheLife: SegmentRefCacheLife,
                    mmap: MMAP.Segment,
                    mergeResult: Iterable[DefIO[SegmentOption, Iterable[T]]])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                              timeOrder: TimeOrder[Slice[Byte]],
                                                                              functionStore: FunctionStore,
                                                                              fileSweeper: FileSweeper,
                                                                              bufferCleaner: ByteBufferSweeperActor,
                                                                              keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                                              blockCacheSweeper: Option[MemorySweeper.Block],
                                                                              segmentReadIO: SegmentReadIO,
                                                                              idGenerator: IDGenerator,
                                                                              forceSaveApplier: ForceSaveApplier): IO[Error.Segment, Iterable[DefIO[SegmentOption, Iterable[S]]]]

  def persistTransient(pathsDistributor: PathsDistributor,
                       segmentRefCacheLife: SegmentRefCacheLife,
                       mmap: MMAP.Segment,
                       transient: Iterable[T])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                               timeOrder: TimeOrder[Slice[Byte]],
                                               functionStore: FunctionStore,
                                               fileSweeper: FileSweeper,
                                               bufferCleaner: ByteBufferSweeperActor,
                                               keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                               blockCacheSweeper: Option[MemorySweeper.Block],
                                               segmentReadIO: SegmentReadIO,
                                               idGenerator: IDGenerator,
                                               forceSaveApplier: ForceSaveApplier): IO[Error.Segment, Iterable[S]]
}

object SegmentWriteIO {
  implicit val persistentSegmentWriteIO: SegmentWritePersistentIO.type = SegmentWritePersistentIO
  implicit val memorySegmentWriteIO: SegmentWriteMemoryIO.type = SegmentWriteMemoryIO
}
