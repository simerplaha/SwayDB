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
import swaydb.core.level.compaction.CompactResult
import swaydb.core.merge.stats.MergeStats
import swaydb.core.segment.SegmentSource._
import swaydb.core.segment._
import swaydb.core.segment.assigner.{Assignable, GapAggregator, SegmentAssigner, SegmentAssignment}
import swaydb.core.segment.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.block.bloomfilter.BloomFilterBlock
import swaydb.core.segment.block.hashindex.HashIndexBlock
import swaydb.core.segment.block.segment.SegmentBlock
import swaydb.core.segment.block.segment.data.TransientSegment
import swaydb.core.segment.block.sortedindex.SortedIndexBlock
import swaydb.core.segment.block.values.ValuesBlock
import swaydb.core.segment.ref.{SegmentRef, SegmentRefOption}
import swaydb.core.util.IDGenerator
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.util.Futures
import swaydb.data.util.Futures._

import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}

/**
 * Runs defragmentation on [[PersistentSegment]] types.
 *
 * It optimises the file [[TransientSegment.Persistent]] such that we defer
 * byte transfer to OS as much as possible and expand segments only if it's
 * too small or have key-values that can be cleared/removed.
 */
object DefragPersistentSegment {

  /**
   * Builds a [[Future]] that executes defragmentation and merge on a single Segment.
   */
  def runOnSegment[SEG, NULL_SEG >: SEG](segment: SEG,
                                         nullSegment: NULL_SEG,
                                         headGap: ListBuffer[Assignable.Gap[MergeStats.Persistent.Builder[Memory, ListBuffer]]],
                                         tailGap: ListBuffer[Assignable.Gap[MergeStats.Persistent.Builder[Memory, ListBuffer]]],
                                         mergeableCount: Int,
                                         mergeable: => Iterator[Assignable],
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
                                                              segmentConfig: SegmentBlock.Config): Future[CompactResult[NULL_SEG, Slice[TransientSegment.Persistent]]] =
    Future {
      Defrag.runOnSegment(
        segment = segment,
        nullSegment = nullSegment,
        fragments = ListBuffer.empty[TransientSegment.Fragment[MergeStats.Persistent.Builder[Memory, ListBuffer]]],
        headGap = headGap,
        tailGap = tailGap,
        mergeableCount = mergeableCount,
        mergeable = mergeable,
        removeDeletes = removeDeletes,
        createdInLevel = createdInLevel,
        createFence = (_: SEG) => TransientSegment.Fence
      )
    } flatMap {
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
   * Builds a [[Future]] that executes defragmentation and merge on a single Segment.
   */
  def runOnGaps[SEG, NULL_SEG >: SEG](nullSegment: NULL_SEG,
                                      headGap: ListBuffer[Assignable.Gap[MergeStats.Persistent.Builder[Memory, ListBuffer]]],
                                      tailGap: ListBuffer[Assignable.Gap[MergeStats.Persistent.Builder[Memory, ListBuffer]]],
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
                                                           segmentConfig: SegmentBlock.Config): Future[CompactResult[NULL_SEG, Slice[TransientSegment.Persistent]]] =
    Defrag.runOnGaps(
      fragments = ListBuffer.empty[TransientSegment.Fragment[MergeStats.Persistent.Builder[Memory, ListBuffer]]],
      headGap = headGap,
      tailGap = tailGap,
      removeDeletes = removeDeletes,
      createdInLevel = createdInLevel,
      fence = TransientSegment.Fence
    ) flatMap {
      mergeResult =>
        commitFragments(
          fragments = mergeResult,
          createdInLevel = createdInLevel
        ) map {
          persistentSegments =>
            CompactResult(nullSegment, persistentSegments)
        }
    }

  /**
   * Builds a [[Future]] pipeline that executes assignment, defragmentation and merge on multiple Segments. This is
   * used by [[PersistentSegmentMany]].
   *
   * @return [[CompactResult.source]] is true if this Segment was replaced or else it will be false.
   *         [[swaydb.core.segment.ref.SegmentRef]] is not being used here because the input is an [[Iterator]] of [[SEG]].
   */
  def runMany(headGap: ListBuffer[Assignable.Gap[MergeStats.Persistent.Builder[Memory, ListBuffer]]],
              tailGap: ListBuffer[Assignable.Gap[MergeStats.Persistent.Builder[Memory, ListBuffer]]],
              segment: PersistentSegmentMany,
              mergeableCount: Int,
              mergeables: Iterator[Assignable],
              removeDeletes: Boolean,
              createdInLevel: Int)(implicit idGenerator: IDGenerator,
                                   executionContext: ExecutionContext,
                                   keyOrder: KeyOrder[Slice[Byte]],
                                   timeOrder: TimeOrder[Slice[Byte]],
                                   functionStore: FunctionStore,
                                   valuesConfig: ValuesBlock.Config,
                                   sortedIndexConfig: SortedIndexBlock.Config,
                                   binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                                   hashIndexConfig: HashIndexBlock.Config,
                                   bloomFilterConfig: BloomFilterBlock.Config,
                                   segmentConfig: SegmentBlock.Config): Future[CompactResult[PersistentSegmentOption, Slice[TransientSegment.Persistent]]] =
    if (mergeableCount == 0)
      DefragPersistentSegment.runOnGaps[PersistentSegmentMany, PersistentSegmentOption](
        nullSegment = PersistentSegment.Null,
        headGap = headGap,
        tailGap = tailGap,
        removeDeletes = removeDeletes,
        createdInLevel = createdInLevel
      )
    else
      Futures
        .unit
        .flatMapUnit {
          runHeadDefragAndAssignments(
            headGap = headGap,
            segments = segment.segmentRefsIterator(),
            mergeableCount = mergeableCount,
            mergeables = mergeables,
            removeDeletes = removeDeletes,
            createdInLevel = createdInLevel
          )
        }
        .flatMap {
          headDefragAndAssignments =>
            defragAssignedAndMergeHead[SegmentRefOption, SegmentRef](
              nullSegment = SegmentRef.Null,
              removeDeletes = removeDeletes,
              createdInLevel = createdInLevel,
              headAndAssignments = headDefragAndAssignments,
              //for PersistentSegmentMany transfer unexpanded Refs so always copy them forward.
              createFence = ref => TransientSegment.RemoteRef(ref)
            )
        }
        .map {
          fragments =>
            //run tail fragments
            if (tailGap.isEmpty)
              fragments
            else
              DefragGap.run(
                gap = tailGap,
                fragments = fragments,
                removeDeletes = removeDeletes,
                createdInLevel = createdInLevel,
                hasNext = false
              )
        }
        .flatMap {
          fragments =>
            commitFragments(
              fragments = fragments,
              createdInLevel = createdInLevel
            ) map {
              transientSegments =>
                CompactResult(
                  source = segment, //replaced
                  result = transientSegments
                )
            }
        }

  case class HeadDefragAndAssignments[SEG >: Null](headFragments: ListBuffer[TransientSegment.Fragment[MergeStats.Persistent.Builder[Memory, ListBuffer]]],
                                                   midAssignments: ListBuffer[SegmentAssignment[ListBuffer[Assignable.Gap[MergeStats.Persistent.Builder[Memory, ListBuffer]]], SEG]])

  /**
   * Run headGap's defragmentation and mid key-values assignment concurrently.
   */
  private def runHeadDefragAndAssignments[NULL_SEG >: SEG, SEG >: Null](headGap: ListBuffer[Assignable.Gap[MergeStats.Persistent.Builder[Memory, ListBuffer]]],
                                                                        segments: => Iterator[SEG],
                                                                        mergeableCount: Int,
                                                                        mergeables: Iterator[Assignable],
                                                                        removeDeletes: Boolean,
                                                                        createdInLevel: Int)(implicit executionContext: ExecutionContext,
                                                                                             keyOrder: KeyOrder[Slice[Byte]],
                                                                                             segmentSource: SegmentSource[SEG],
                                                                                             sortedIndexConfig: SortedIndexBlock.Config,
                                                                                             segmentConfig: SegmentBlock.Config): Future[HeadDefragAndAssignments[SEG]] = {
    val headFragmentsFuture =
      if (headGap.isEmpty)
        Future.successful(ListBuffer.empty[TransientSegment.Fragment[MergeStats.Persistent.Builder[Memory, ListBuffer]]])
      else
        Future {
          DefragGap.run(
            gap = headGap,
            fragments = ListBuffer.empty[TransientSegment.Fragment[MergeStats.Persistent.Builder[Memory, ListBuffer]]],
            removeDeletes = removeDeletes,
            createdInLevel = createdInLevel,
            hasNext = false
          )
        }

    val assignmentsFuture =
      Future {
        assignAllSegments(
          segments = segments,
          assignableCount = mergeableCount,
          mergeables = mergeables,
          removeDeletes = removeDeletes
        )
      }

    for {
      headFragments <- headFragmentsFuture
      assignments <- assignmentsFuture
    } yield HeadDefragAndAssignments(headFragments, assignments)
  }

  /**
   * Run defragmentation on assigned key-values and combine headGap's fragments.
   */
  private def defragAssignedAndMergeHead[NULL_SEG >: SEG, SEG >: Null](nullSegment: NULL_SEG,
                                                                       removeDeletes: Boolean,
                                                                       createdInLevel: Int,
                                                                       headAndAssignments: HeadDefragAndAssignments[SEG],
                                                                       createFence: SEG => TransientSegment.Fragment[MergeStats.Persistent.Builder[Memory, ListBuffer]])(implicit segmentSource: SegmentSource[SEG],
                                                                                                                                                                         keyOrder: KeyOrder[Slice[Byte]],
                                                                                                                                                                         timeOrder: TimeOrder[Slice[Byte]],
                                                                                                                                                                         functionStore: FunctionStore,
                                                                                                                                                                         executionContext: ExecutionContext,
                                                                                                                                                                         sortedIndexConfig: SortedIndexBlock.Config,
                                                                                                                                                                         segmentConfig: SegmentBlock.Config): Future[ListBuffer[TransientSegment.Fragment[MergeStats.Persistent.Builder[Memory, ListBuffer]]]] =
    Future.traverse(headAndAssignments.midAssignments) {
      assignment =>
        //Segments that did not get assign a key-value should be converted to Fragment straight away.
        if (assignment.headGap.result.isEmpty && assignment.tailGap.result.isEmpty && assignment.midOverlap.isEmpty)
          segmentSource match {
            case SegmentSource.SegmentTarget =>
              val remoteSegment =
                TransientSegment.RemotePersistentSegment(segment = assignment.segment.asInstanceOf[PersistentSegment])

              Future.successful(ListBuffer(remoteSegment))

            case SegmentSource.SegmentRefTarget =>
              val remoteRef = TransientSegment.RemoteRef(assignment.segment.asInstanceOf[SegmentRef])
              Future.successful(ListBuffer(remoteRef))
          }
        else
          Future {
            Defrag.runOnSegment(
              segment = assignment.segment,
              nullSegment = nullSegment,
              fragments = ListBuffer.empty[TransientSegment.Fragment[MergeStats.Persistent.Builder[Memory, ListBuffer]]],
              headGap = assignment.headGap.result,
              tailGap = assignment.tailGap.result,
              mergeableCount = assignment.midOverlap.size,
              mergeable = assignment.midOverlap.iterator,
              removeDeletes = removeDeletes,
              createdInLevel = createdInLevel,
              createFence = createFence
            ).result
          }
    } map {
      buffer =>
        if (headAndAssignments.headFragments.isEmpty)
          buffer.flatten
        else
          headAndAssignments.headFragments ++= buffer.flatten
    }

  /**
   * Assigns key-values [[Assignable]]s to segments [[SEG]].
   *
   * This also re-assigns Segments ([[SEG]]) that were not to assigned to any of the assignables
   * so all Segments are merge.
   *
   * This function is primary used by [[PersistentSegmentMany]] and assigning [[SegmentRef]] and
   * is NOT used by [[swaydb.core.level.Level]].
   */
  def assignAllSegments[SEG >: Null](segments: Iterator[SEG],
                                     assignableCount: Int,
                                     mergeables: Iterator[Assignable],
                                     removeDeletes: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                             segmentSource: SegmentSource[SEG]): ListBuffer[SegmentAssignment[ListBuffer[Assignable.Gap[MergeStats.Persistent.Builder[Memory, ListBuffer]]], SEG]] = {
    implicit val creator: Aggregator.Creator[Assignable, ListBuffer[Assignable.Gap[MergeStats.Persistent.Builder[Memory, ListBuffer]]]] =
      GapAggregator.create(removeDeletes)

    val (segmentsIterator, segmentsIteratorDuplicate) = segments.duplicate

    //assign key-values to Segment and then perform merge.
    val assignments =
      SegmentAssigner.assignUnsafeGaps[ListBuffer[Assignable.Gap[MergeStats.Persistent.Builder[Memory, ListBuffer]]], SEG](
        assignablesCount = assignableCount,
        assignables = mergeables,
        segments = segmentsIterator
      )

    val hasMissing =
      segmentsIteratorDuplicate.foldLeft(false) {
        case (missing, segment) =>
          //TODO - assignments dont need to check for added missing assignments.
          if (!assignments.exists(_.segment == segment)) {
            assignments +=
              SegmentAssignment(
                segment = segment,
                headGap = creator.createNew(),
                midOverlap = ListBuffer.empty,
                tailGap = creator.createNew()
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

  /**
   * Converts [[MergeStats]] to [[TransientSegment.One]] instances for grouping.
   */
  def commitFragments(fragments: ListBuffer[TransientSegment.Fragment[MergeStats.Persistent.Builder[Memory, ListBuffer]]],
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

          SegmentBlock.writeOnes(
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
        TransientSegment.Fence.futureSuccessful

    } flatMap {
      singletonOrFence =>
        commitFencedSingletons(
          singletonOrFence = singletonOrFence,
          createdInLevel = createdInLevel
        )
    }

  /**
   * Groups [[TransientSegment.Singleton]] for persistence and does a final check and expand small Segments.
   */
  def commitFencedSingletons(singletonOrFence: ListBuffer[Slice[TransientSegment.SingletonOrFence]],
                             createdInLevel: Int)(implicit executionContext: ExecutionContext,
                                                  keyOrder: KeyOrder[Slice[Byte]],
                                                  valuesConfig: ValuesBlock.Config,
                                                  sortedIndexConfig: SortedIndexBlock.Config,
                                                  binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                                                  hashIndexConfig: HashIndexBlock.Config,
                                                  segmentConfig: SegmentBlock.Config): Future[Slice[TransientSegment.Persistent]] = {
    val singletonOrFences = Slice.of[TransientSegment.SingletonOrFence](singletonOrFence.foldLeft(0)(_ + _.size))
    singletonOrFence foreach singletonOrFences.addAll

    val groups = ListBuffer(ListBuffer.empty[TransientSegment.OneOrRemoteRef])

    val remoteSegments = ListBuffer.empty[TransientSegment.RemotePersistentSegment]

    @inline def startNewGroup(): Unit =
      if (groups.last.nonEmpty)
        groups += ListBuffer.empty

    //do another check and expand small Segments.
    singletonOrFences foreach {
      case ref: TransientSegment.RemoteRef =>
        groups.last += ref

      case remoteSegment: TransientSegment.RemotePersistentSegment =>
        if (remoteSegment.segment.segmentSize < segmentConfig.minSize) {
          remoteSegment.segment match {
            case many: PersistentSegmentMany =>
              many.segmentRefsIterator() foreach (ref => groups.last += TransientSegment.RemoteRef(ref))

            case one: PersistentSegmentOne =>
              groups.last += TransientSegment.RemoteRef(one.ref)
          }
        } else {
          startNewGroup()
          remoteSegments += remoteSegment
        }

      case one: TransientSegment.One =>
        groups.last += one

      case TransientSegment.Fence =>
        startNewGroup()
    }

    commitGroups(
      groups = groups,
      remoteSegments = remoteSegments,
      createdInLevel = createdInLevel
    )
  }

  /**
   * Commits Groups.
   */
  private def commitGroups(groups: Iterable[Iterable[TransientSegment.OneOrRemoteRef]],
                           remoteSegments: Iterable[TransientSegment.RemotePersistentSegment],
                           createdInLevel: Int)(implicit executionContext: ExecutionContext,
                                                keyOrder: KeyOrder[Slice[Byte]],
                                                valuesConfig: ValuesBlock.Config,
                                                sortedIndexConfig: SortedIndexBlock.Config,
                                                binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                                                hashIndexConfig: HashIndexBlock.Config,
                                                segmentConfig: SegmentBlock.Config): Future[Slice[TransientSegment.Persistent]] =
    Future.traverse(groups) {
      ones =>
        Future {
          SegmentBlock.writeOneOrMany(
            createdInLevel = createdInLevel,
            ones = Slice.from(ones, ones.size),
            sortedIndexConfig = sortedIndexConfig,
            hashIndexConfig = hashIndexConfig,
            binarySearchIndexConfig = binarySearchIndexConfig,
            valuesConfig = valuesConfig,
            segmentConfig = segmentConfig
          )
        }
    } map {
      slices =>
        val slice = Slice.of[TransientSegment.Persistent](slices.foldLeft(remoteSegments.size)(_ + _.size))

        @inline def addAll(segments: Iterable[TransientSegment.Persistent]): Unit =
          segments foreach {
            case ref: TransientSegment.RemoteRef =>
              //if a single SegmentRef is being written as an independent Segment then set the header.
              slice add ref.copyWithFileHeader(PersistentSegmentOne.formatIdSlice)

            case transient =>
              slice add transient
          }

        slices foreach addAll

        if (remoteSegments.isEmpty) {
          slice
        } else {
          addAll(remoteSegments)
          slice.sortBy(_.minKey)(keyOrder)
        }
    }
}
