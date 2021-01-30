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

package swaydb.core.segment.defrag

import swaydb.core.data.{DefIO, Memory}
import swaydb.core.function.FunctionStore
import swaydb.core.level.PathsDistributor
import swaydb.core.merge.stats.MergeStats
import swaydb.core.segment._
import swaydb.core.segment.assigner.Assignable
import swaydb.core.segment.block.segment.SegmentBlock
import swaydb.core.segment.block.segment.data.TransientSegment
import swaydb.core.sweeper.FileSweeper
import swaydb.core.util.IDGenerator
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice

import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}

object DefragMemorySegment {

  /**
   * Default [[MergeStats.Memory]] and build new [[MemorySegment]].
   */
  def runOnSegment[SEG, NULL_SEG >: SEG](segment: SEG,
                                         nullSegment: NULL_SEG,
                                         headGap: Iterable[Assignable.Gap[MergeStats.Memory.Builder[Memory, ListBuffer]]],
                                         tailGap: Iterable[Assignable.Gap[MergeStats.Memory.Builder[Memory, ListBuffer]]],
                                         newKeyValues: => Iterator[Assignable],
                                         removeDeletes: Boolean,
                                         createdInLevel: Int,
                                         pathsDistributor: PathsDistributor)(implicit executionContext: ExecutionContext,
                                                                             keyOrder: KeyOrder[Slice[Byte]],
                                                                             timeOrder: TimeOrder[Slice[Byte]],
                                                                             functionStore: FunctionStore,
                                                                             defragSource: DefragSource[SEG],
                                                                             segmentConfig: SegmentBlock.Config,
                                                                             fileSweeper: FileSweeper,
                                                                             idGenerator: IDGenerator): Future[DefIO[NULL_SEG, Iterable[MemorySegment]]] =
    Future {
      Defrag.runOnSegment(
        segment = segment,
        nullSegment = nullSegment,
        fragments = ListBuffer.empty[TransientSegment.Fragment[MergeStats.Memory.Builder[Memory, ListBuffer]]],
        headGap = headGap,
        tailGap = tailGap,
        newKeyValues = newKeyValues,
        removeDeletes = removeDeletes,
        createdInLevel = createdInLevel,
        createFence = (_: SEG) => TransientSegment.Fence
      )
    } flatMap {
      mergeResult =>
        commitSegments(
          mergeResult = mergeResult.output,
          removeDeletes = removeDeletes,
          createdInLevel = createdInLevel,
          pathsDistributor = pathsDistributor
        ) map {
          result =>
            mergeResult.withOutput(result.flatten)
        }
    }

  /**
   * Default [[MergeStats.Memory]] and build new [[MemorySegment]].
   */
  def runOnGaps[SEG, NULL_SEG >: SEG](nullSegment: NULL_SEG,
                                      headGap: Iterable[Assignable.Gap[MergeStats.Memory.Builder[Memory, ListBuffer]]],
                                      tailGap: Iterable[Assignable.Gap[MergeStats.Memory.Builder[Memory, ListBuffer]]],
                                      removeDeletes: Boolean,
                                      createdInLevel: Int,
                                      pathsDistributor: PathsDistributor)(implicit executionContext: ExecutionContext,
                                                                          keyOrder: KeyOrder[Slice[Byte]],
                                                                          timeOrder: TimeOrder[Slice[Byte]],
                                                                          functionStore: FunctionStore,
                                                                          defragSource: DefragSource[SEG],
                                                                          segmentConfig: SegmentBlock.Config,
                                                                          fileSweeper: FileSweeper,
                                                                          idGenerator: IDGenerator): Future[DefIO[NULL_SEG, Iterable[MemorySegment]]] =
    Defrag.runOnGaps(
      fragments = ListBuffer.empty[TransientSegment.Fragment[MergeStats.Memory.Builder[Memory, ListBuffer]]],
      headGap = headGap,
      tailGap = tailGap,
      removeDeletes = removeDeletes,
      createdInLevel = createdInLevel,
      fence = TransientSegment.Fence
    ) flatMap {
      mergeResult =>
        commitSegments(
          mergeResult = mergeResult,
          removeDeletes = removeDeletes,
          createdInLevel = createdInLevel,
          pathsDistributor = pathsDistributor
        ) map {
          result =>
            DefIO(
              input = nullSegment,
              output = result.flatten
            )
        }
    }

  /**
   * Converts all de-fragmented [[TransientSegment.Fragment]]s to [[MemorySegment]].
   *
   * Unlike [[DefragPersistentSegment]] which converts [[TransientSegment.Fragment]] to
   * [[TransientSegment.Persistent]] which then get persisted via [[swaydb.core.segment.io.SegmentWritePersistentIO]]
   * here [[TransientSegment.Fragment]]s are converted directly to [[MemorySegment]] instead of converting them within
   * [[swaydb.core.segment.io.SegmentWriteMemoryIO]] because we can have more concurrency here as
   * [[swaydb.core.segment.io.SegmentWriteIO]] instances are not concurrent.
   */
  private def commitSegments[NULL_SEG >: SEG, SEG](mergeResult: ListBuffer[TransientSegment.Fragment[MergeStats.Memory.Builder[Memory, ListBuffer]]],
                                                   removeDeletes: Boolean,
                                                   createdInLevel: Int,
                                                   pathsDistributor: PathsDistributor)(implicit executionContext: ExecutionContext,
                                                                                       keyOrder: KeyOrder[Slice[Byte]],
                                                                                       timeOrder: TimeOrder[Slice[Byte]],
                                                                                       functionStore: FunctionStore,
                                                                                       segmentConfig: SegmentBlock.Config,
                                                                                       fileSweeper: FileSweeper,
                                                                                       idGenerator: IDGenerator): Future[ListBuffer[Slice[MemorySegment]]] =
    Future.traverse(mergeResult) {
      case remote: TransientSegment.Remote =>
        Future {
          remote match {
            case ref: TransientSegment.RemoteRef =>
              Segment.copyToMemory(
                keyValues = ref.iterator(),
                pathsDistributor = pathsDistributor,
                removeDeletes = removeDeletes,
                minSegmentSize = segmentConfig.minSize,
                maxKeyValueCountPerSegment = segmentConfig.maxCount,
                createdInLevel = createdInLevel
              )

            case TransientSegment.RemotePersistentSegment(segment) =>
              Segment.copyToMemory(
                segment = segment,
                createdInLevel = createdInLevel,
                pathsDistributor = pathsDistributor,
                removeDeletes = removeDeletes,
                minSegmentSize = segmentConfig.minSize,
                maxKeyValueCountPerSegment = segmentConfig.maxCount
              )
          }
        }

      case TransientSegment.Stats(stats) =>
        if (stats.isEmpty)
          Future.successful(Slice.empty)
        else
          Future {
            Segment.memory(
              minSegmentSize = segmentConfig.minSize,
              maxKeyValueCountPerSegment = segmentConfig.maxCount,
              pathsDistributor = pathsDistributor,
              createdInLevel = createdInLevel,
              stats = stats.close
            )
          }

      case TransientSegment.Fence =>
        Future.successful(Slice.empty)
    }
}
