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

import swaydb.Aggregator
import swaydb.core.data.{DefIO, Memory}
import swaydb.core.function.FunctionStore
import swaydb.core.io.file.ForceSaveApplier
import swaydb.core.level.PathsDistributor
import swaydb.core.level.compaction.io.CompactionIO
import swaydb.core.merge.stats.MergeStats
import swaydb.core.segment._
import swaydb.core.segment.assigner._
import swaydb.core.segment.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.block.bloomfilter.BloomFilterBlock
import swaydb.core.segment.block.hashindex.HashIndexBlock
import swaydb.core.segment.block.segment.SegmentBlock
import swaydb.core.segment.block.segment.data.TransientSegment
import swaydb.core.segment.block.sortedindex.SortedIndexBlock
import swaydb.core.segment.block.values.ValuesBlock
import swaydb.core.segment.defrag.DefragSource._
import swaydb.core.segment.io.{SegmentReadIO, SegmentWriteIO}
import swaydb.core.segment.ref.{SegmentRef, SegmentRefOption}
import swaydb.core.sweeper.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.sweeper.{FileSweeper, MemorySweeper}
import swaydb.core.util.IDGenerator
import swaydb.data.compaction.CompactionConfig.CompactionParallelism
import swaydb.data.config.{MMAP, SegmentRefCacheLife}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.utils.Futures
import swaydb.utils.Futures.FutureUnitImplicits

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}

/**
 * heap, mid and tail key-values could be of any size. The following defragmentation
 * ensures that no Segments are too small or too large.
 *
 * It also attempts to defer byte transfer to OS as much as possible and expands
 * segments only if it's too small or have key-values that can be cleared/removed.
 */
object DefragPersistentSegment {

  /**
   * Builds a [[Future]] that executes defragmentation and merge on a single Segment.
   */
  def runOnSegment[SEG, NULL_SEG >: SEG](segment: SEG,
                                         nullSegment: NULL_SEG,
                                         headGap: Iterable[Assignable.Gap[MergeStats.Persistent.Builder[Memory, ListBuffer]]],
                                         tailGap: Iterable[Assignable.Gap[MergeStats.Persistent.Builder[Memory, ListBuffer]]],
                                         newKeyValues: Iterator[Assignable],
                                         removeDeletes: Boolean,
                                         createdInLevel: Int,
                                         pathsDistributor: PathsDistributor,
                                         segmentRefCacheLife: SegmentRefCacheLife,
                                         mmap: MMAP.Segment)(implicit executionContext: ExecutionContext,
                                                             defragSource: DefragSource[SEG],
                                                             keyOrder: KeyOrder[Slice[Byte]],
                                                             valuesConfig: ValuesBlock.Config,
                                                             sortedIndexConfig: SortedIndexBlock.Config,
                                                             binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                                                             hashIndexConfig: HashIndexBlock.Config,
                                                             bloomFilterConfig: BloomFilterBlock.Config,
                                                             segmentConfig: SegmentBlock.Config,
                                                             timeOrder: TimeOrder[Slice[Byte]],
                                                             functionStore: FunctionStore,
                                                             fileSweeper: FileSweeper,
                                                             bufferCleaner: ByteBufferSweeperActor,
                                                             keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                             blockCacheSweeper: Option[MemorySweeper.Block],
                                                             segmentReadIO: SegmentReadIO,
                                                             idGenerator: IDGenerator,
                                                             forceSaveApplier: ForceSaveApplier,
                                                             compactionIO: CompactionIO.Actor,
                                                             segmentWriteIO: SegmentWriteIO[TransientSegment.Persistent, PersistentSegment],
                                                             compactionParallelism: CompactionParallelism): Future[DefIO[NULL_SEG, scala.collection.SortedSet[PersistentSegment]]] =
    Future {
      Defrag.runOnSegment(
        segment = segment,
        nullSegment = nullSegment,
        fragments = ListBuffer.empty[TransientSegment.Fragment[MergeStats.Persistent.Builder[Memory, ListBuffer]]],
        headGap = headGap,
        tailGap = tailGap,
        newKeyValues = newKeyValues,
        removeDeletes = removeDeletes,
        createdInLevel = createdInLevel,
        createFence = (_: SEG) => TransientSegment.Fence
      )
    } flatMap {
      mergeResult =>
        commitFragments(
          fragments = mergeResult.output,
          createdInLevel = createdInLevel,
          pathsDistributor = pathsDistributor,
          segmentRefCacheLife = segmentRefCacheLife,
          mmap = mmap
        ) map {
          persistentSegments =>
            mergeResult.withOutput(persistentSegments)
        }
    }

  /**
   * Builds a [[Future]] that executes defragmentation and merge on a single Segment.
   */
  def runOnGaps[SEG, NULL_SEG >: SEG](nullSegment: NULL_SEG,
                                      headGap: Iterable[Assignable.Gap[MergeStats.Persistent.Builder[Memory, ListBuffer]]],
                                      tailGap: Iterable[Assignable.Gap[MergeStats.Persistent.Builder[Memory, ListBuffer]]],
                                      removeDeletes: Boolean,
                                      createdInLevel: Int,
                                      pathsDistributor: PathsDistributor,
                                      segmentRefCacheLife: SegmentRefCacheLife,
                                      mmap: MMAP.Segment)(implicit executionContext: ExecutionContext,
                                                          keyOrder: KeyOrder[Slice[Byte]],
                                                          valuesConfig: ValuesBlock.Config,
                                                          sortedIndexConfig: SortedIndexBlock.Config,
                                                          binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                                                          hashIndexConfig: HashIndexBlock.Config,
                                                          bloomFilterConfig: BloomFilterBlock.Config,
                                                          segmentConfig: SegmentBlock.Config,
                                                          timeOrder: TimeOrder[Slice[Byte]],
                                                          functionStore: FunctionStore,
                                                          fileSweeper: FileSweeper,
                                                          bufferCleaner: ByteBufferSweeperActor,
                                                          keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                          blockCacheSweeper: Option[MemorySweeper.Block],
                                                          segmentReadIO: SegmentReadIO,
                                                          idGenerator: IDGenerator,
                                                          forceSaveApplier: ForceSaveApplier,
                                                          compactionIO: CompactionIO.Actor,
                                                          segmentWriteIO: SegmentWriteIO[TransientSegment.Persistent, PersistentSegment],
                                                          compactionParallelism: CompactionParallelism): Future[DefIO[NULL_SEG, scala.collection.SortedSet[PersistentSegment]]] =
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
          createdInLevel = createdInLevel,
          pathsDistributor = pathsDistributor,
          segmentRefCacheLife = segmentRefCacheLife,
          mmap = mmap
        ) map {
          persistentSegments =>
            DefIO(
              input = nullSegment,
              output = persistentSegments
            )
        }
    }

  /**
   * Builds a [[Future]] pipeline that executes assignment, defragmentation and merge on multiple Segments. This is
   * used by [[PersistentSegmentMany]].
   *
   * @return [[DefIO.input]] is true if this Segment was replaced or else it will be false.
   *         [[swaydb.core.segment.ref.SegmentRef]] is not being used here because the input is an [[Iterator]] of [[SEG]].
   */
  def runMany(headGap: Iterable[Assignable.Gap[MergeStats.Persistent.Builder[Memory, ListBuffer]]],
              tailGap: Iterable[Assignable.Gap[MergeStats.Persistent.Builder[Memory, ListBuffer]]],
              segment: PersistentSegmentMany,
              newKeyValues: Iterator[Assignable],
              removeDeletes: Boolean,
              createdInLevel: Int,
              pathsDistributor: PathsDistributor,
              segmentRefCacheLife: SegmentRefCacheLife,
              mmap: MMAP.Segment)(implicit executionContext: ExecutionContext,
                                  keyOrder: KeyOrder[Slice[Byte]],
                                  valuesConfig: ValuesBlock.Config,
                                  sortedIndexConfig: SortedIndexBlock.Config,
                                  binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                                  hashIndexConfig: HashIndexBlock.Config,
                                  bloomFilterConfig: BloomFilterBlock.Config,
                                  segmentConfig: SegmentBlock.Config,
                                  timeOrder: TimeOrder[Slice[Byte]],
                                  functionStore: FunctionStore,
                                  fileSweeper: FileSweeper,
                                  bufferCleaner: ByteBufferSweeperActor,
                                  keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                  blockCacheSweeper: Option[MemorySweeper.Block],
                                  segmentReadIO: SegmentReadIO,
                                  idGenerator: IDGenerator,
                                  forceSaveApplier: ForceSaveApplier,
                                  compactionIO: CompactionIO.Actor,
                                  segmentWriteIO: SegmentWriteIO[TransientSegment.Persistent, PersistentSegment],
                                  compactionParallelism: CompactionParallelism): Future[DefIO[PersistentSegmentOption, scala.collection.SortedSet[PersistentSegment]]] =
    if (newKeyValues.isEmpty)
      DefragPersistentSegment.runOnGaps[PersistentSegmentMany, PersistentSegmentOption](
        nullSegment = PersistentSegment.Null,
        headGap = headGap,
        tailGap = tailGap,
        removeDeletes = removeDeletes,
        createdInLevel = createdInLevel,
        pathsDistributor = pathsDistributor,
        segmentRefCacheLife = segmentRefCacheLife,
        mmap = mmap
      )
    else
      Future
        .unit
        .flatMapUnit {
          runHeadDefragAndAssignments(
            headGap = headGap,
            segments = segment.segmentRefs(segmentConfig.initialiseIteratorsInOneSeek),
            newKeyValues = newKeyValues,
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
              headFragmentsAndAssignments = headDefragAndAssignments,
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
              createdInLevel = createdInLevel,
              pathsDistributor = pathsDistributor,
              segmentRefCacheLife = segmentRefCacheLife,
              mmap = mmap
            ) map {
              transientSegments =>
                DefIO(
                  input = segment, //replaced
                  output = transientSegments
                )
            }
        }

  /**
   * Run headGap's defragmentation and mid key-values assignment concurrently.
   */
  private def runHeadDefragAndAssignments[NULL_SEG >: SEG, SEG >: Null](headGap: Iterable[Assignable.Gap[MergeStats.Persistent.Builder[Memory, ListBuffer]]],
                                                                        segments: Iterator[SEG],
                                                                        newKeyValues: Iterator[Assignable],
                                                                        removeDeletes: Boolean,
                                                                        createdInLevel: Int)(implicit executionContext: ExecutionContext,
                                                                                             keyOrder: KeyOrder[Slice[Byte]],
                                                                                             assignmentTarget: AssignmentTarget[SEG],
                                                                                             defragSource: DefragSource[SEG],
                                                                                             sortedIndexConfig: SortedIndexBlock.Config,
                                                                                             segmentConfig: SegmentBlock.Config): Future[FragmentAndAssignment[SEG]] = {
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
          newKeyValues = newKeyValues,
          removeDeletes = removeDeletes,
          initialiseIteratorsInOneSeek = segmentConfig.initialiseIteratorsInOneSeek
        )
      }

    for {
      headFragments <- headFragmentsFuture
      assignments <- assignmentsFuture
    } yield FragmentAndAssignment(headFragments, assignments)
  }

  /**
   * Run defragmentation on assigned key-values and combine headGap's fragments.
   */
  private def defragAssignedAndMergeHead[NULL_SEG >: SEG, SEG >: Null](nullSegment: NULL_SEG,
                                                                       removeDeletes: Boolean,
                                                                       createdInLevel: Int,
                                                                       headFragmentsAndAssignments: FragmentAndAssignment[SEG],
                                                                       createFence: SEG => TransientSegment.Fragment[MergeStats.Persistent.Builder[Memory, ListBuffer]])(implicit defragSource: DefragSource[SEG],
                                                                                                                                                                         keyOrder: KeyOrder[Slice[Byte]],
                                                                                                                                                                         timeOrder: TimeOrder[Slice[Byte]],
                                                                                                                                                                         functionStore: FunctionStore,
                                                                                                                                                                         executionContext: ExecutionContext,
                                                                                                                                                                         sortedIndexConfig: SortedIndexBlock.Config,
                                                                                                                                                                         segmentConfig: SegmentBlock.Config,
                                                                                                                                                                         compactionParallelism: CompactionParallelism): Future[ListBuffer[TransientSegment.Fragment[MergeStats.Persistent.Builder[Memory, ListBuffer]]]] =
    Futures.traverseBounded(compactionParallelism.groupedSegmentDefragParallelism, headFragmentsAndAssignments.assignments) {
      assignment =>
        //Segments that did not get assign a key-value should be converted to Fragment straight away.
        //but if the segment is required for cleanup feed it to defrag so that expired key-values get cleared.
        if (assignment.headGap.result.isEmpty && assignment.tailGap.result.isEmpty && assignment.midOverlap.result.isEmpty && (!removeDeletes || !assignment.segment.hasUpdateOrRangeOrExpired))
          defragSource match {
            case DefragSource.SegmentTarget =>
              val remoteSegment =
                TransientSegment.RemotePersistentSegment(segment = assignment.segment.asInstanceOf[PersistentSegment])

              Future.successful(ListBuffer(remoteSegment))

            case DefragSource.SegmentRefTarget =>
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
              newKeyValues = assignment.midOverlap.result.iterator,
              removeDeletes = removeDeletes,
              createdInLevel = createdInLevel,
              createFence = createFence
            ).output
          }
    } map {
      buffers =>
        if (headFragmentsAndAssignments.fragments.isEmpty)
          buffers.flatten
        else
          buffers.foldLeft(headFragmentsAndAssignments.fragments) {
            case (flattened, next) =>
              flattened ++= next
          }
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
                                     newKeyValues: Iterator[Assignable],
                                     removeDeletes: Boolean,
                                     initialiseIteratorsInOneSeek: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                            assignmentTarget: AssignmentTarget[SEG],
                                                                            defragSource: DefragSource[SEG]): ListBuffer[Assignment[ListBuffer[Assignable.Gap[MergeStats.Persistent.Builder[Memory, ListBuffer]]], ListBuffer[Assignable], SEG]] = {
    implicit val creator: Aggregator.Creator[Assignable, ListBuffer[Assignable.Gap[MergeStats.Persistent.Builder[Memory, ListBuffer]]]] =
      GapAggregator.create(removeDeletes)

    val (segmentsIterator, segmentsIteratorDuplicate) = segments.duplicate

    //assign key-values to Segment and then perform merge.
    val assignments =
      Assigner.assignUnsafeGaps[ListBuffer[Assignable.Gap[MergeStats.Persistent.Builder[Memory, ListBuffer]]], ListBuffer[Assignable], SEG](
        keyValues = newKeyValues,
        segments = segmentsIterator,
        initialiseIteratorsInOneSeek = initialiseIteratorsInOneSeek
      )

    val hasMissing =
      segmentsIteratorDuplicate.foldLeft(false) {
        case (missing, segment) =>
          //TODO - assignments dont need to check for added missing assignments.
          if (!assignments.exists(_.segment == segment)) {
            assignments +=
              Assignment(
                segment = segment,
                headGap = creator.createNew(),
                midOverlap = Aggregator.listBuffer,
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
   * Groups [[TransientSegment.Singleton]] for persistence and does a final check and expand small Segments
   * remove fences.
   */
  def commitFragments(fragments: ListBuffer[TransientSegment.Fragment[MergeStats.Persistent.Builder[Memory, ListBuffer]]],
                      createdInLevel: Int,
                      pathsDistributor: PathsDistributor,
                      segmentRefCacheLife: SegmentRefCacheLife,
                      mmap: MMAP.Segment)(implicit executionContext: ExecutionContext,
                                          keyOrder: KeyOrder[Slice[Byte]],
                                          valuesConfig: ValuesBlock.Config,
                                          sortedIndexConfig: SortedIndexBlock.Config,
                                          binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                                          hashIndexConfig: HashIndexBlock.Config,
                                          bloomFilterConfig: BloomFilterBlock.Config,
                                          segmentConfig: SegmentBlock.Config,
                                          timeOrder: TimeOrder[Slice[Byte]],
                                          functionStore: FunctionStore,
                                          fileSweeper: FileSweeper,
                                          bufferCleaner: ByteBufferSweeperActor,
                                          keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                          blockCacheSweeper: Option[MemorySweeper.Block],
                                          segmentReadIO: SegmentReadIO,
                                          idGenerator: IDGenerator,
                                          forceSaveApplier: ForceSaveApplier,
                                          compactionIO: CompactionIO.Actor,
                                          segmentWriteIO: SegmentWriteIO[TransientSegment.Persistent, PersistentSegment],
                                          compactionParallelism: CompactionParallelism): Future[scala.collection.SortedSet[PersistentSegment]] = {
    val groups = ListBuffer.empty[ListBuffer[TransientSegment.RemoteRefOrStats[MergeStats.Persistent.Builder[Memory, ListBuffer]]]]

    val remoteSegments = ListBuffer.empty[TransientSegment.RemotePersistentSegment]

    @inline def startNewGroup(): Unit =
      if (groups.isEmpty || groups.last.nonEmpty)
        groups += ListBuffer.empty

    @inline def addLast(last: TransientSegment.RemoteRefOrStats[MergeStats.Persistent.Builder[Memory, ListBuffer]]) =
      if (groups.isEmpty)
        groups += ListBuffer(last)
      else
        groups.last += last

    val fragmentsIterator = fragments.iterator

    while (fragmentsIterator.hasNext)
      fragmentsIterator.next() match {
        case ref: TransientSegment.RemoteRef =>
          addLast(ref)

        case remoteSegment: TransientSegment.RemotePersistentSegment =>
          if (remoteSegment.segment.segmentSize < segmentConfig.minSize) {
            remoteSegment.segment match {
              case many: PersistentSegmentMany =>
                many.segmentRefs(segmentConfig.initialiseIteratorsInOneSeek) foreach (ref => addLast(TransientSegment.RemoteRef(ref)))

              case one: PersistentSegmentOne =>
                addLast(TransientSegment.RemoteRef(one.ref))
            }
          } else {
            startNewGroup()
            remoteSegments += remoteSegment
          }

        case one @ TransientSegment.Stats(_) =>
          addLast(one)

        case TransientSegment.Fence =>
          if (fragmentsIterator.hasNext)
            startNewGroup()
      }

    def runMerge(buffer: mutable.SortedSet[PersistentSegment]): Future[mutable.SortedSet[PersistentSegment]] =
      if (groups.isEmpty)
        Future.successful(buffer)
      else
        Futures.traverseBounded(compactionParallelism.defragmentedSegmentParallelism, groups) {
          group =>
            commitGroup(
              group = group,
              createdInLevel = createdInLevel,
              pathsDistributor = pathsDistributor,
              segmentRefCacheLife = segmentRefCacheLife,
              mmap = mmap
            )
        } map {
          newSegments =>
            for (segments <- newSegments)
              buffer ++= segments

            buffer
        }

    runMerge(mutable.SortedSet.empty(Ordering.by[PersistentSegment, Slice[Byte]](_.minKey)(keyOrder))) flatMap {
      mergedSegments =>
        if (remoteSegments.isEmpty)
          Future.successful(mergedSegments)
        else
          compactionIO.ask.flatMap {
            (instance, _) =>
              instance.persist(
                pathsDistributor = pathsDistributor,
                segmentRefCacheLife = segmentRefCacheLife,
                mmap = mmap,
                transient = remoteSegments
              )
          } map {
            persistedRemote =>
              mergedSegments ++= persistedRemote
          }
    }
  }

  /**
   * Commits Groups.
   */
  private def commitGroup(group: Iterable[TransientSegment.RemoteRefOrStats[MergeStats.Persistent.Builder[Memory, ListBuffer]]],
                          createdInLevel: Int,
                          pathsDistributor: PathsDistributor,
                          segmentRefCacheLife: SegmentRefCacheLife,
                          mmap: MMAP.Segment)(implicit executionContext: ExecutionContext,
                                              keyOrder: KeyOrder[Slice[Byte]],
                                              valuesConfig: ValuesBlock.Config,
                                              sortedIndexConfig: SortedIndexBlock.Config,
                                              binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                                              hashIndexConfig: HashIndexBlock.Config,
                                              bloomFilterConfig: BloomFilterBlock.Config,
                                              segmentConfig: SegmentBlock.Config,
                                              timeOrder: TimeOrder[Slice[Byte]],
                                              functionStore: FunctionStore,
                                              fileSweeper: FileSweeper,
                                              bufferCleaner: ByteBufferSweeperActor,
                                              keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                              blockCacheSweeper: Option[MemorySweeper.Block],
                                              segmentReadIO: SegmentReadIO,
                                              idGenerator: IDGenerator,
                                              forceSaveApplier: ForceSaveApplier,
                                              compactionIO: CompactionIO.Actor,
                                              segmentWriteIO: SegmentWriteIO[TransientSegment.Persistent, PersistentSegment],
                                              compactionParallelism: CompactionParallelism): Future[Iterable[PersistentSegment]] =
    if (group.isEmpty)
      Futures.emptyIterable
    else
      Futures.traverseBounded(compactionParallelism.defragmentedSegmentParallelism, group) {
        case ref: TransientSegment.RemoteRef =>
          Future.successful(Slice(ref))

        case TransientSegment.Stats(stats) =>
          Future
            .unit
            .mapUnit {
              stats.close(
                hasAccessPositionIndex = sortedIndexConfig.enableAccessPositionIndex,
                optimiseForReverseIteration = sortedIndexConfig.optimiseForReverseIteration
              )
            }
            .flatMap {
              mergeStats =>
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
      } flatMap {
        segments =>
          SegmentBlock.writeOneOrMany(
            createdInLevel = createdInLevel,
            ones = Slice.from(segments),
            sortedIndexConfig = sortedIndexConfig,
            hashIndexConfig = hashIndexConfig,
            binarySearchIndexConfig = binarySearchIndexConfig,
            valuesConfig = valuesConfig,
            segmentConfig = segmentConfig
          )
      } flatMap {
        transient =>
          compactionIO.ask.flatMap {
            (instance, _) =>
              instance.persist(
                pathsDistributor = pathsDistributor,
                segmentRefCacheLife = segmentRefCacheLife,
                mmap = mmap,
                transient = transient
              )
          }
      }
}
