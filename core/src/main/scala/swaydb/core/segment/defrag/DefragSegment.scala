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
import swaydb.Aggregator.nothingAggregator
import swaydb.core.data.{Memory, MergeResult}
import swaydb.core.function.FunctionStore
import swaydb.core.merge.MergeStats
import swaydb.core.merge.MergeStats.Persistent
import swaydb.core.segment.SegmentSource
import swaydb.core.segment.SegmentSource._
import swaydb.core.segment.assigner.{Assignable, GapAggregator, SegmentAssigner, SegmentAssignment}
import swaydb.core.segment.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.block.bloomfilter.BloomFilterBlock
import swaydb.core.segment.block.hashindex.HashIndexBlock
import swaydb.core.segment.block.segment.SegmentBlock
import swaydb.core.segment.block.segment.data.TransientSegment
import swaydb.core.segment.block.sortedindex.SortedIndexBlock
import swaydb.core.segment.block.values.ValuesBlock
import swaydb.core.util.IDGenerator
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.util.Futures
import swaydb.data.util.Futures._

import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}

object DefragSegment {

  def run[SEG, NULL_SEG >: SEG](segment: Option[SEG],
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
                                                     segmentConfig: SegmentBlock.Config): Future[MergeResult[NULL_SEG, Slice[TransientSegment.Persistent]]] =
    Futures
      .unit
      .mapUnit {
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
        )
      }
      .flatMap {
        mergeResult =>
          commitFragments(
            fragments = mergeResult.result,
            createdInLevel = createdInLevel
          ) map {
            persistentSegments =>
              mergeResult.updateResult(persistentSegments)
          }
      }

  /**
   * @return [[MergeResult.source]] is true if this Segment was replaced or else it will be false.
   *         [[swaydb.core.segment.ref.SegmentRef]] is not being used here because the input is an [[Iterator]] of [[SEG]].
   */
  def run[SEG >: Null, NULL_SEG >: SEG](headGap: ListBuffer[Assignable.Gap[MergeStats.Persistent.Builder[Memory, ListBuffer]]],
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
                                                             segmentConfig: SegmentBlock.Config): Future[MergeResult[Boolean, Slice[TransientSegment.Persistent]]] =
    if (assignableCount == 0)
      DefragSegment.run[SEG, NULL_SEG](
        segment = Option.empty[SEG],
        nullSegment = nullSegment,
        headGap = headGap,
        tailGap = tailGap,
        mergeableCount = 0,
        mergeable = Assignable.emptyIterator,
        removeDeletes = removeDeletes,
        createdInLevel = createdInLevel
      ) map {
        result =>
          assert(result.source == nullSegment, s"${result.source} is not $nullSegment")
          result.updateSource(false)
      }
    else
      Futures
        .unit
        .flatMapUnit {
          val headFragmentsFuture =
            if (headGap.isEmpty)
              Future.successful(ListBuffer.empty[TransientSegment.Fragment])
            else
              Future {
                DefragGap.run(
                  gap = headGap,
                  fragments = ListBuffer.empty,
                  removeDeletes = removeDeletes,
                  createdInLevel = createdInLevel
                )
              }

          val assignmentsFuture =
            Future {
              assignAllSegments(
                segments = segments,
                assignableCount = assignableCount,
                assignables = assignables,
                removeDeletes = removeDeletes
              )
            }

          for {
            headFragments <- headFragmentsFuture
            assignments <- assignmentsFuture
          } yield (headFragments, assignments)

        }
        .flatMap {
          case (headFragments, assignments) =>
            Future.traverse(assignments) {
              assignment =>
                Future {
                  Defrag.run(
                    segment = Some(assignment.segment),
                    nullSegment = nullSegment,
                    fragments = ListBuffer.empty,
                    headGap = assignment.headGap.result,
                    tailGap = assignment.tailGap.result,
                    mergeableCount = assignment.midOverlap.size,
                    mergeable = assignment.midOverlap.iterator,
                    removeDeletes = removeDeletes,
                    createdInLevel = createdInLevel
                  )
                }
            } map {
              buffer =>
                headFragments ++= buffer.flatMap(_.result)
            }
        }
        .map {
          fragments: ListBuffer[TransientSegment.Fragment] =>
            if (tailGap.isEmpty)
              fragments
            else
              DefragGap.run(
                gap = tailGap,
                fragments = fragments,
                removeDeletes = removeDeletes,
                createdInLevel = createdInLevel
              )
        }
        .flatMap {
          fragments =>
            commitFragments(
              fragments = fragments,
              createdInLevel = createdInLevel
            ) map {
              transientSegments =>
                MergeResult(
                  source = true, //replaced
                  result = transientSegments
                )
            }
        }

  def commitFragments(fragments: ListBuffer[TransientSegment.Fragment],
                      createdInLevel: Int)(implicit executionContext: ExecutionContext,
                                           keyOrder: KeyOrder[Slice[Byte]],
                                           timeOrder: TimeOrder[Slice[Byte]],
                                           functionStore: FunctionStore,
                                           valuesConfig: ValuesBlock.Config,
                                           sortedIndexConfig: SortedIndexBlock.Config,
                                           binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                                           hashIndexConfig: HashIndexBlock.Config,
                                           bloomFilterConfig: BloomFilterBlock.Config,
                                           segmentConfig: SegmentBlock.Config): Future[Slice[TransientSegment.Persistent]] =
    Future.traverse(fragments) {
      case TransientSegment.Stats(stats) =>
        Future {
          val mergeStats =
            stats.close(
              hasAccessPositionIndex = sortedIndexConfig.enableAccessPositionIndex,
              optimiseForReverseIteration = sortedIndexConfig.optimiseForReverseIteration
            )

          SegmentBlock.writeOneOrMany(
            mergeStats = mergeStats,
            createdInLevel = createdInLevel,
            bloomFilterConfig = bloomFilterConfig,
            hashIndexConfig = hashIndexConfig,
            binarySearchIndexConfig = binarySearchIndexConfig,
            sortedIndexConfig = sortedIndexConfig,
            valuesConfig = valuesConfig,
            segmentConfig = segmentConfig
          )
        }

      case segment: TransientSegment.Remote =>
        Future.successful(Slice(segment))

      case TransientSegment.Fence =>
        Future.successful(Slice.empty)

    } map {
      buffer =>
        //TODO - group SegmentRefs and write them as PersistentSegmentMany if needed.
        val slice = Slice.of[TransientSegment.Persistent](buffer.foldLeft(0)(_ + _.size))
        buffer foreach slice.addAll
        slice
    }

  def assignAllSegments[SEG >: Null](segments: Iterator[SEG],
                                     assignableCount: Int,
                                     assignables: Iterator[Assignable],
                                     removeDeletes: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                             segmentSource: SegmentSource[SEG]): ListBuffer[SegmentAssignment[ListBuffer[Assignable.Gap[Persistent.Builder[Memory, ListBuffer]]], SEG]] = {
    implicit val creator: Aggregator.Creator[Assignable, ListBuffer[Assignable.Gap[Persistent.Builder[Memory, ListBuffer]]]] =
      GapAggregator.persistent(removeDeletes)

    val (segmentsIterator, segmentsIteratorDuplicate) = segments.duplicate

    //assign key-values to Segment and then perform merge.
    val assignments =
      SegmentAssigner.assignUnsafeGaps[ListBuffer[Assignable.Gap[MergeStats.Persistent.Builder[Memory, ListBuffer]]], SEG](
        assignablesCount = assignableCount,
        assignables = assignables,
        segments = segmentsIterator
      )

    val nothingAssignableGap = nothingAggregator[Assignable]()

    val hasMissing =
      segmentsIteratorDuplicate.foldLeft(false) {
        case (missing, segment) =>
          if (!assignments.exists(_.segment == segment)) {
            assignments +=
              SegmentAssignment(
                segment = segment,
                headGap = nothingAssignableGap,
                midOverlap = ListBuffer.empty,
                tailGap = nothingAssignableGap
              )

            true
          } else {
            missing
          }
      }

    if (hasMissing)
      assignments.sortBy(_.segment.minKey)(keyOrder)
    else
      assignments
  }
}
