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

package swaydb.core.segment.defrag

import swaydb.Aggregator
import swaydb.core.data.Memory
import swaydb.core.function.FunctionStore
import swaydb.core.merge.MergeStats
import swaydb.core.merge.MergeStats.Persistent
import swaydb.core.segment.SegmentSource
import swaydb.core.segment.assigner.{Assignable, GapAggregator, SegmentAssigner, SegmentAssignment}
import swaydb.core.segment.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.block.bloomfilter.BloomFilterBlock
import swaydb.core.segment.block.hashindex.HashIndexBlock
import swaydb.core.segment.block.segment.SegmentBlock
import swaydb.core.segment.block.segment.data.TransientSegment
import swaydb.core.segment.block.sortedindex.SortedIndexBlock
import swaydb.core.segment.block.values.ValuesBlock
import swaydb.core.segment.ref.SegmentMergeResult
import swaydb.core.util.IDGenerator
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.util.Futures
import swaydb.data.util.Futures._

import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}

object DefragMaterialiser {

  def materialise[SEG, NULL_SEG >: SEG](segment: Option[SEG],
                                        nullSegment: NULL_SEG,
                                        headGap: ListBuffer[Assignable.Gap[MergeStats.Persistent.Builder[Memory, ListBuffer]]],
                                        tailGap: ListBuffer[Assignable.Gap[MergeStats.Persistent.Builder[Memory, ListBuffer]]],
                                        mergeableCount: Int,
                                        mergeable: Iterator[Assignable],
                                        removeDeletes: Boolean,
                                        createdInLevel: Int)(implicit executionContext: ExecutionContext,
                                                             keyOrder: KeyOrder[Slice[Byte]],
                                                             timeOrder: TimeOrder[Slice[Byte]],
                                                             functionStore: FunctionStore,
                                                             segmentSource: SegmentSource[SEG],
                                                             valuesConfig: ValuesBlock.Config,
                                                             sortedIndexConfig: SortedIndexBlock.Config,
                                                             binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                                                             hashIndexConfig: HashIndexBlock.Config,
                                                             bloomFilterConfig: BloomFilterBlock.Config,
                                                             segmentConfig: SegmentBlock.Config): Future[SegmentMergeResult[NULL_SEG, Slice[TransientSegment.Persistent]]] =
    Defrag.run(
      segment = segment,
      nullSegment = nullSegment,
      fragments = ListBuffer.empty,
      headGap = headGap,
      tailGap = tailGap,
      mergeableCount = mergeableCount,
      mergeable = mergeable,
      removeDeletes = removeDeletes,
      createdInLevel = createdInLevel
    ) flatMap {
      mergeResult =>
        Defrag.materialise(
          fragments = mergeResult,
          createdInLevel = createdInLevel
        )
    }


  def assignMaterialise[SEG >: Null, NULL_SEG >: SEG](headGap: ListBuffer[Assignable.Gap[MergeStats.Persistent.Builder[Memory, ListBuffer]]],
                                                      tailGap: ListBuffer[Assignable.Gap[MergeStats.Persistent.Builder[Memory, ListBuffer]]],
                                                      nullSegment: NULL_SEG,
                                                      segments: => Iterator[SEG],
                                                      assignableCount: Int,
                                                      assignables: Iterator[Assignable],
                                                      removeDeletes: Boolean,
                                                      createdInLevel: Int)(implicit idGenerator: IDGenerator,
                                                                           executionContext: ExecutionContext,
                                                                           keyOrder: KeyOrder[Slice[Byte]],
                                                                           timeOrder: TimeOrder[Slice[Byte]],
                                                                           functionStore: FunctionStore,
                                                                           segmentSource: SegmentSource[SEG],
                                                                           valuesConfig: ValuesBlock.Config,
                                                                           sortedIndexConfig: SortedIndexBlock.Config,
                                                                           binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                                                                           hashIndexConfig: HashIndexBlock.Config,
                                                                           bloomFilterConfig: BloomFilterBlock.Config,
                                                                           segmentConfig: SegmentBlock.Config): Future[SegmentMergeResult[NULL_SEG, Slice[TransientSegment.Persistent]]] =
    if (assignableCount == 0)
      DefragMaterialiser.materialise[SEG, NULL_SEG](
        segment = Option.empty[SEG],
        nullSegment = nullSegment,
        headGap = headGap,
        tailGap = tailGap,
        mergeableCount = 0,
        mergeable = Assignable.emptyIterator,
        removeDeletes = removeDeletes,
        createdInLevel = createdInLevel
      )
    else
      Futures
        .unit
        .flatMapUnit {
          if (headGap.isEmpty)
            Future.successful(ListBuffer.empty[SegmentAssignment[ListBuffer[Assignable.Gap[MergeStats.Persistent.Builder[Memory, ListBuffer]]], SEG]], ListBuffer.empty[TransientSegment.Fragment])
          else
            Future {
              val fragments =
                DefragGap.run(
                  gap = headGap,
                  fragments = ListBuffer.empty,
                  removeDeletes = removeDeletes,
                  createdInLevel = createdInLevel
                )

              implicit val creator: Aggregator.Creator[Assignable, ListBuffer[Assignable.Gap[Persistent.Builder[Memory, ListBuffer]]]] =
                GapAggregator.persistentCreator(removeDeletes)

              //assign key-values to Segment and then perform merge.
              val assignments =
                SegmentAssigner.assignUnsafeGaps[ListBuffer[Assignable.Gap[MergeStats.Persistent.Builder[Memory, ListBuffer]]], SEG](
                  assignablesCount = assignableCount,
                  assignables = assignables,
                  segments = segments
                )

              (assignments, fragments)
            }
        }
        .flatMap {
          case (assignments, fragments) =>
            Future.traverse(assignments) {
              assignment =>
                Defrag.run(
                  segment = Some(assignment.segment),
                  nullSegment = nullSegment,
                  fragments = fragments,
                  headGap = assignment.headGap.result,
                  tailGap = assignment.tailGap.result,
                  mergeableCount = assignment.midOverlap.size,
                  mergeable = assignment.midOverlap.iterator,
                  removeDeletes = removeDeletes,
                  createdInLevel = createdInLevel
                )
            }
        }
        .flatMap {
          mergeResult: ListBuffer[SegmentMergeResult[NULL_SEG, ListBuffer[TransientSegment.Fragment]]] =>
            if (tailGap.isEmpty)
              Future.successful(mergeResult)
            else
              Future {
                val result =
                  DefragGap.run(
                    gap = tailGap,
                    fragments = ListBuffer.empty,
                    removeDeletes = removeDeletes,
                    createdInLevel = createdInLevel
                  )
                mergeResult += SegmentMergeResult(nullSegment, result)
              }
        }
        .flatMap {
          result =>
            Defrag.materialise(
              fragments = ???,
              createdInLevel = createdInLevel
            )
        }

}
