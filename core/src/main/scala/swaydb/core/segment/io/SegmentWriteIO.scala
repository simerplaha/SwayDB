/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.core.segment.io

import swaydb.core.function.FunctionStore
import swaydb.core.io.file.ForceSaveApplier
import swaydb.core.level.PathsDistributor
import swaydb.core.level.compaction.CompactResult
import swaydb.core.segment._
import swaydb.core.segment.block.segment.data.TransientSegment
import swaydb.core.sweeper.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.sweeper.{FileSweeper, MemorySweeper}
import swaydb.core.util.IDGenerator
import swaydb.data.config.MMAP
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.{Error, IO}

/**
 * Provides implementation on how [[TransientSegment]]
 * should be persisted.
 *
 * Used by [[swaydb.core.level.compaction.Compaction]] to delay persisting
 * [[TransientSegment]] on dedicated IO ExecutionContext.
 *
 */
trait SegmentWriteIO[T <: TransientSegment, S] {

  def minKey(segment: S): Slice[Byte]

  def persistMerged(pathsDistributor: PathsDistributor,
                    segmentRefCacheWeight: Int,
                    mmap: MMAP.Segment,
                    mergeResult: Iterable[CompactResult[SegmentOption, Iterable[T]]])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                                      timeOrder: TimeOrder[Slice[Byte]],
                                                                                      functionStore: FunctionStore,
                                                                                      fileSweeper: FileSweeper,
                                                                                      bufferCleaner: ByteBufferSweeperActor,
                                                                                      keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                                                      blockCacheSweeper: Option[MemorySweeper.Block],
                                                                                      segmentReadIO: SegmentReadIO,
                                                                                      idGenerator: IDGenerator,
                                                                                      forceSaveApplier: ForceSaveApplier): IO[Error.Segment, Iterable[CompactResult[SegmentOption, Iterable[S]]]]

  def persistTransient(pathsDistributor: PathsDistributor,
                       segmentRefCacheWeight: Int,
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
