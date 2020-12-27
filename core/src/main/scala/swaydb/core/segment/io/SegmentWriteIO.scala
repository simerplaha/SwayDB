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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.core.segment.io

import swaydb.Error.Segment.ExceptionHandler
import swaydb.core.actor.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.actor.{FileSweeper, MemorySweeper}
import swaydb.core.function.FunctionStore
import swaydb.core.io.file.ForceSaveApplier
import swaydb.core.level.PathsDistributor
import swaydb.core.level.compaction.CompactResult
import swaydb.core.segment._
import swaydb.core.segment.block.segment.data.TransientSegment
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


object SegmentWriteIO extends SegmentWriteIO[TransientSegment, Segment] {

  override def minKey(segment: Segment): Slice[Byte] =
    segment.minKey

  override def persistTransient(pathsDistributor: PathsDistributor,
                                segmentRefCacheWeight: Int,
                                mmap: MMAP.Segment,
                                transient: Iterable[TransientSegment])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                       timeOrder: TimeOrder[Slice[Byte]],
                                                                       functionStore: FunctionStore,
                                                                       fileSweeper: FileSweeper,
                                                                       bufferCleaner: ByteBufferSweeperActor,
                                                                       keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                                       blockCacheSweeper: Option[MemorySweeper.Block],
                                                                       segmentReadIO: SegmentReadIO,
                                                                       idGenerator: IDGenerator,
                                                                       forceSaveApplier: ForceSaveApplier): IO[Error.Segment, Iterable[Segment]] = {
    val transientIterator = transient.iterator

    if (!transientIterator.hasNext) {
      IO.Right(Slice.empty)
    } else {
      val headOrNull = transientIterator.next()

      headOrNull match {
        case _: TransientSegment.Persistent =>
          SegmentWritePersistentIO.persistTransient(
            pathsDistributor = pathsDistributor,
            segmentRefCacheWeight = segmentRefCacheWeight,
            mmap = mmap,
            transient = transient.asInstanceOf[Iterable[TransientSegment.Persistent]]
          )

        case _: TransientSegment.Memory =>
          SegmentWriteMemoryIO.persistTransient(
            pathsDistributor = pathsDistributor,
            segmentRefCacheWeight = segmentRefCacheWeight,
            mmap = mmap,
            transient = transient.asInstanceOf[Iterable[TransientSegment.Memory]]
          )
      }
    }
  }

  override def persistMerged(pathsDistributor: PathsDistributor,
                             segmentRefCacheWeight: Int,
                             mmap: MMAP.Segment,
                             mergeResult: Iterable[CompactResult[SegmentOption, Iterable[TransientSegment]]])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                                                              timeOrder: TimeOrder[Slice[Byte]],
                                                                                                              functionStore: FunctionStore,
                                                                                                              fileSweeper: FileSweeper,
                                                                                                              bufferCleaner: ByteBufferSweeperActor,
                                                                                                              keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                                                                              blockCacheSweeper: Option[MemorySweeper.Block],
                                                                                                              segmentReadIO: SegmentReadIO,
                                                                                                              idGenerator: IDGenerator,
                                                                                                              forceSaveApplier: ForceSaveApplier): IO[Error.Segment, Iterable[CompactResult[SegmentOption, Iterable[Segment]]]] = {
    val transientIterator = mergeResult.iterator

    if (!transientIterator.hasNext) {
      IO.Right(Slice.empty)
    } else {
      val headOrNull = transientIterator.next().result.head

      headOrNull match {
        case _: TransientSegment.Persistent =>
          SegmentWritePersistentIO.persistMerged(
            pathsDistributor = pathsDistributor,
            segmentRefCacheWeight = segmentRefCacheWeight,
            mmap = mmap,
            mergeResult = mergeResult.asInstanceOf[Iterable[CompactResult[SegmentOption, Iterable[TransientSegment.Persistent]]]]
          )

        case _: TransientSegment.Memory =>
          SegmentWriteMemoryIO.persistMerged(
            pathsDistributor = pathsDistributor,
            segmentRefCacheWeight = segmentRefCacheWeight,
            mmap = mmap,
            mergeResult = mergeResult.asInstanceOf[Iterable[CompactResult[SegmentOption, Iterable[TransientSegment.Memory]]]]
          )
      }
    }
  }

}
