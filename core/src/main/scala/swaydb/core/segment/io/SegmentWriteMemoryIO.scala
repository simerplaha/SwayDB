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

import swaydb.Error.Segment.ExceptionHandler
import swaydb.core.data.DefIO
import swaydb.core.function.FunctionStore
import swaydb.core.file.ForceSaveApplier
import swaydb.core.level.PathsDistributor
import swaydb.core.segment._
import swaydb.core.segment.block.segment.transient.TransientSegment
import swaydb.core.sweeper.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.sweeper.{FileSweeper, MemorySweeper}
import swaydb.core.util.IDGenerator
import swaydb.config.{MMAP, SegmentRefCacheLife}
import swaydb.slice.order.{KeyOrder, TimeOrder}
import swaydb.slice.Slice
import swaydb.{Error, IO}


object SegmentWriteMemoryIO extends SegmentWriteIO[TransientSegment.Memory, MemorySegment] {

  override def minKey(segment: MemorySegment): Slice[Byte] =
    segment.minKey

  override def persistTransient(pathsDistributor: PathsDistributor,
                                segmentRefCacheLife: SegmentRefCacheLife,
                                mmap: MMAP.Segment,
                                transient: Iterable[TransientSegment.Memory])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                              timeOrder: TimeOrder[Slice[Byte]],
                                                                              functionStore: FunctionStore,
                                                                              fileSweeper: FileSweeper,
                                                                              bufferCleaner: ByteBufferSweeperActor,
                                                                              keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                                              blockCacheSweeper: Option[MemorySweeper.Block],
                                                                              segmentReadIO: SegmentReadIO,
                                                                              idGenerator: IDGenerator,
                                                                              forceSaveApplier: ForceSaveApplier): IO[Error.Segment, Iterable[MemorySegment]] =
    IO.Right {
      transient.map(_.segment)
    }

  override def persistMerged(pathsDistributor: PathsDistributor,
                             segmentRefCacheLife: SegmentRefCacheLife,
                             mmap: MMAP.Segment,
                             mergeResult: Iterable[DefIO[SegmentOption, Iterable[TransientSegment.Memory]]])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                                                             timeOrder: TimeOrder[Slice[Byte]],
                                                                                                             functionStore: FunctionStore,
                                                                                                             fileSweeper: FileSweeper,
                                                                                                             bufferCleaner: ByteBufferSweeperActor,
                                                                                                             keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                                                                             blockCacheSweeper: Option[MemorySweeper.Block],
                                                                                                             segmentReadIO: SegmentReadIO,
                                                                                                             idGenerator: IDGenerator,
                                                                                                             forceSaveApplier: ForceSaveApplier): IO[Error.Segment, Iterable[DefIO[SegmentOption, Iterable[MemorySegment]]]] =
    IO.Right {
      mergeResult collect {
        //collect the ones with source set or has new segments to write
        case mergeResult if mergeResult.input.isSomeS || mergeResult.output.nonEmpty =>
          val segments = mergeResult.output.map(_.segment)
          mergeResult.withOutput(segments)
      }
    }
}
