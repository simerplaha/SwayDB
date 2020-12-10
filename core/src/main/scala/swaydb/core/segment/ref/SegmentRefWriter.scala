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

package swaydb.core.segment.ref

import com.typesafe.scalalogging.LazyLogging
import swaydb.Aggregator
import swaydb.core.data.{KeyValue, Memory, Persistent}
import swaydb.core.function.FunctionStore
import swaydb.core.segment.Segment
import swaydb.core.segment.assigner.{Assignable, SegmentAssigner}
import swaydb.core.segment.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.block.bloomfilter.BloomFilterBlock
import swaydb.core.segment.block.hashindex.HashIndexBlock
import swaydb.core.segment.block.segment.SegmentBlock
import swaydb.core.segment.block.segment.data.TransientSegment
import swaydb.core.segment.block.sortedindex.SortedIndexBlock
import swaydb.core.segment.block.values.ValuesBlock
import swaydb.core.segment.merge.{MergeStats, SegmentMerger}
import swaydb.core.util.IDGenerator
import swaydb.data.compaction.ParallelMerge.SegmentParallelism
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice

import scala.collection.mutable.ListBuffer
import scala.concurrent.{Await, ExecutionContext}

object SegmentRefWriter extends LazyLogging {

  def merge(oldKeyValuesCount: Int,
            oldKeyValues: Iterator[Persistent],
            headGap: Iterable[Assignable],
            tailGap: Iterable[Assignable],
            mergeableCount: Int,
            mergeable: Iterator[Assignable],
            removeDeletes: Boolean,
            createdInLevel: Int,
            valuesConfig: ValuesBlock.Config,
            sortedIndexConfig: SortedIndexBlock.Config,
            binarySearchIndexConfig: BinarySearchIndexBlock.Config,
            hashIndexConfig: HashIndexBlock.Config,
            bloomFilterConfig: BloomFilterBlock.Config,
            segmentConfig: SegmentBlock.Config)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                timeOrder: TimeOrder[Slice[Byte]],
                                                functionStore: FunctionStore): Slice[TransientSegment.OneOrMany] = {

    val builder = MergeStats.persistent[Memory, ListBuffer](Aggregator.listBuffer)

    SegmentMerger.merge(
      headGap = headGap,
      tailGap = tailGap,
      mergeableCount = mergeableCount,
      mergeable = mergeable,
      oldKeyValuesCount = oldKeyValuesCount,
      oldKeyValues = oldKeyValues,
      stats = builder,
      isLastLevel = removeDeletes
    )

    val closed =
      builder.close(
        hasAccessPositionIndex = sortedIndexConfig.enableAccessPositionIndex,
        optimiseForReverseIteration = sortedIndexConfig.optimiseForReverseIteration
      )

    SegmentBlock.writeOneOrMany(
      mergeStats = closed,
      createdInLevel = createdInLevel,
      bloomFilterConfig = bloomFilterConfig,
      hashIndexConfig = hashIndexConfig,
      binarySearchIndexConfig = binarySearchIndexConfig,
      sortedIndexConfig = sortedIndexConfig,
      valuesConfig = valuesConfig,
      segmentConfig = segmentConfig
    )
  }

  def mergeOne(oldKeyValuesCount: Int,
               oldKeyValues: Iterator[Persistent],
               headGap: Iterable[Assignable],
               tailGap: Iterable[Assignable],
               mergeableCount: Int,
               mergeable: Iterator[Assignable],
               removeDeletes: Boolean,
               createdInLevel: Int,
               valuesConfig: ValuesBlock.Config,
               sortedIndexConfig: SortedIndexBlock.Config,
               binarySearchIndexConfig: BinarySearchIndexBlock.Config,
               hashIndexConfig: HashIndexBlock.Config,
               bloomFilterConfig: BloomFilterBlock.Config,
               segmentConfig: SegmentBlock.Config)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                   timeOrder: TimeOrder[Slice[Byte]],
                                                   functionStore: FunctionStore): Slice[TransientSegment.One] = {

    val builder = MergeStats.persistent[Memory, ListBuffer](Aggregator.listBuffer)

    SegmentMerger.merge(
      headGap = headGap,
      tailGap = tailGap,
      mergeableCount = mergeableCount,
      mergeable = mergeable,
      oldKeyValuesCount = oldKeyValuesCount,
      oldKeyValues = oldKeyValues,
      stats = builder,
      isLastLevel = removeDeletes
    )

    val closed =
      builder.close(
        hasAccessPositionIndex = sortedIndexConfig.enableAccessPositionIndex,
        optimiseForReverseIteration = sortedIndexConfig.optimiseForReverseIteration
      )

    SegmentBlock.writeOnes(
      mergeStats = closed,
      createdInLevel = createdInLevel,
      bloomFilterConfig = bloomFilterConfig,
      hashIndexConfig = hashIndexConfig,
      binarySearchIndexConfig = binarySearchIndexConfig,
      sortedIndexConfig = sortedIndexConfig,
      valuesConfig = valuesConfig,
      segmentConfig = segmentConfig
    )
  }

  def mergeRemoteSegmentAware(assignable: Iterable[Assignable],
                              removeDeletes: Boolean,
                              createdInLevel: Int,
                              valuesConfig: ValuesBlock.Config,
                              sortedIndexConfig: SortedIndexBlock.Config,
                              binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                              hashIndexConfig: HashIndexBlock.Config,
                              bloomFilterConfig: BloomFilterBlock.Config,
                              segmentConfig: SegmentBlock.Config)(implicit keyOrder: KeyOrder[Slice[Byte]]): Slice[TransientSegment.SingletonOrMany] = {

    val stats = MergeStats.persistent[KeyValue, ListBuffer](Aggregator.listBuffer)(_.toMemory())

    val newSegments = ListBuffer.empty[TransientSegment.SingletonOrMany]

    assignable foreach {
      case collection: Assignable.Collection =>
        collection match {
          case segment: Segment =>
            newSegments +=
              TransientSegment.RemoteSegment(
                segment,
                createdInLevel = createdInLevel,
                removeDeletes = removeDeletes,
                valuesConfig = valuesConfig,
                sortedIndexConfig = sortedIndexConfig,
                binarySearchIndexConfig = binarySearchIndexConfig,
                hashIndexConfig = hashIndexConfig,
                bloomFilterConfig = bloomFilterConfig,
                segmentConfig = segmentConfig
              )

          case _ =>
            collection.iterator() foreach stats.add
        }

      case value: KeyValue =>
        stats add value
    }

    val mergeStats =
      stats.close(
        hasAccessPositionIndex = sortedIndexConfig.enableAccessPositionIndex,
        optimiseForReverseIteration = sortedIndexConfig.optimiseForReverseIteration
      )

    val gapedSegments =
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

    (gapedSegments ++ newSegments).sortBy(_.minKey)(keyOrder)
  }

  /**
   * GOAL: The goal of this is to avoid create small segments and to parallelise only if
   * there is enough work for two threads otherwise do the work in the current thread.
   *
   * On failure perform clean up.
   */
  def fastMerge(ref: SegmentRef,
                headGap: Iterable[Assignable],
                tailGap: Iterable[Assignable],
                mergeableCount: Int,
                mergeable: Iterator[Assignable],
                removeDeletes: Boolean,
                createdInLevel: Int,
                segmentParallelism: SegmentParallelism,
                valuesConfig: ValuesBlock.Config,
                sortedIndexConfig: SortedIndexBlock.Config,
                binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                hashIndexConfig: HashIndexBlock.Config,
                bloomFilterConfig: BloomFilterBlock.Config,
                segmentConfig: SegmentBlock.Config)(implicit executionContext: ExecutionContext,
                                                    keyOrder: KeyOrder[Slice[Byte]],
                                                    timeOrder: TimeOrder[Slice[Byte]],
                                                    functionStore: FunctionStore): SegmentMergeResult[Slice[TransientSegment.SingletonOrMany]] = {

    def putGap(gap: Iterable[Assignable]): Slice[TransientSegment.SingletonOrMany] =
      mergeRemoteSegmentAware(
        assignable = gap,
        removeDeletes = removeDeletes,
        createdInLevel = createdInLevel,
        valuesConfig = valuesConfig,
        sortedIndexConfig = sortedIndexConfig,
        binarySearchIndexConfig = binarySearchIndexConfig,
        hashIndexConfig = hashIndexConfig,
        bloomFilterConfig = bloomFilterConfig,
        segmentConfig = segmentConfig
      )

    //if mergeable is empty min gap requirement is 0 so that Segments get copied else
    //there should be at least of quarter or 100 key-values for gaps to be created.
    val minGapSize = if (mergeableCount == 0) 0 else (mergeableCount / 4) max 100

    val (mergeableHead, mergeableTail, future) =
      Segment.runOnGapsParallel[Slice[TransientSegment.SingletonOrMany]](
        headGap = headGap,
        tailGap = tailGap,
        empty = Slice.empty,
        minGapSize = minGapSize,
        segmentParallelism = segmentParallelism
      )(thunk = putGap)

    def putMid(): Slice[TransientSegment.OneOrMany] =
      SegmentRefWriter.merge(
        oldKeyValuesCount = ref.getKeyValueCount(),
        oldKeyValues = ref.iterator(),
        headGap = mergeableHead,
        tailGap = mergeableTail,
        mergeableCount = mergeableCount,
        mergeable = mergeable,
        removeDeletes = removeDeletes,
        createdInLevel = createdInLevel,
        valuesConfig = valuesConfig,
        sortedIndexConfig = sortedIndexConfig,
        binarySearchIndexConfig = binarySearchIndexConfig,
        hashIndexConfig = hashIndexConfig,
        bloomFilterConfig = bloomFilterConfig,
        segmentConfig = segmentConfig
      )

    if (mergeableCount > 0) {
      val midSegments = putMid()
      val (headSegments, tailSegments) = Await.result(future, segmentParallelism.timeout)

      val newSegments = Slice.of[TransientSegment.SingletonOrMany](headSegments.size + midSegments.size + tailSegments.size)
      newSegments addAll headSegments
      newSegments addAll midSegments
      newSegments addAll tailSegments

      SegmentMergeResult(result = newSegments, replaced = true)
    } else {
      val (headSegments, tailSegments) = Await.result(future, segmentParallelism.timeout)

      val newSegments = headSegments ++ tailSegments

      SegmentMergeResult(result = newSegments, replaced = false)
    }
  }

  def fastMergeOne(assignables: Iterable[Assignable],
                   createdInLevel: Int,
                   valuesConfig: ValuesBlock.Config,
                   sortedIndexConfig: SortedIndexBlock.Config,
                   binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                   hashIndexConfig: HashIndexBlock.Config,
                   bloomFilterConfig: BloomFilterBlock.Config,
                   segmentConfig: SegmentBlock.Config)(implicit executionContext: ExecutionContext,
                                                       keyOrder: KeyOrder[Slice[Byte]]): ListBuffer[TransientSegment.One] = {

    val stats = MergeStats.persistent[KeyValue, ListBuffer](Aggregator.listBuffer)(_.toMemory())

    val finalOnes = ListBuffer.empty[TransientSegment.One]

    assignables foreach {
      case collection: Assignable.Collection =>
        val collectionStats = MergeStats.persistent[KeyValue, ListBuffer](Aggregator.listBuffer)(_.toMemory())
        collection.iterator() foreach collectionStats.add

        SegmentBlock.writeOnes(
          mergeStats =
            collectionStats.close(
              hasAccessPositionIndex = sortedIndexConfig.enableAccessPositionIndex,
              optimiseForReverseIteration = sortedIndexConfig.optimiseForReverseIteration
            ),
          createdInLevel = createdInLevel,
          bloomFilterConfig = bloomFilterConfig,
          hashIndexConfig = hashIndexConfig,
          binarySearchIndexConfig = binarySearchIndexConfig,
          sortedIndexConfig = sortedIndexConfig,
          valuesConfig = valuesConfig,
          segmentConfig = segmentConfig
        ) foreach {
          segment =>
            finalOnes += segment
        }

      case value: KeyValue =>
        stats add value
    }

    SegmentBlock.writeOnes(
      mergeStats =
        stats.close(
          hasAccessPositionIndex = sortedIndexConfig.enableAccessPositionIndex,
          optimiseForReverseIteration = sortedIndexConfig.optimiseForReverseIteration
        ),
      createdInLevel = createdInLevel,
      bloomFilterConfig = bloomFilterConfig,
      hashIndexConfig = hashIndexConfig,
      binarySearchIndexConfig = binarySearchIndexConfig,
      sortedIndexConfig = sortedIndexConfig,
      valuesConfig = valuesConfig,
      segmentConfig = segmentConfig
    ) foreach {
      segment =>
        finalOnes += segment
    }

    finalOnes
  }

  def fastMergeOne(ref: SegmentRef,
                   headGap: Iterable[Assignable],
                   tailGap: Iterable[Assignable],
                   mergeableCount: Int,
                   mergeable: Iterator[Assignable],
                   removeDeletes: Boolean,
                   createdInLevel: Int,
                   segmentParallelism: SegmentParallelism,
                   valuesConfig: ValuesBlock.Config,
                   sortedIndexConfig: SortedIndexBlock.Config,
                   binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                   hashIndexConfig: HashIndexBlock.Config,
                   bloomFilterConfig: BloomFilterBlock.Config,
                   segmentConfig: SegmentBlock.Config)(implicit executionContext: ExecutionContext,
                                                       keyOrder: KeyOrder[Slice[Byte]],
                                                       timeOrder: TimeOrder[Slice[Byte]],
                                                       functionStore: FunctionStore): SegmentMergeResult[ListBuffer[TransientSegment.One]] = {

    def writeGap(gap: Iterable[Assignable]): ListBuffer[TransientSegment.One] =
      fastMergeOne(
        assignables = gap,
        createdInLevel = createdInLevel,
        valuesConfig = valuesConfig,
        sortedIndexConfig = sortedIndexConfig,
        binarySearchIndexConfig = binarySearchIndexConfig,
        hashIndexConfig = hashIndexConfig,
        bloomFilterConfig = bloomFilterConfig,
        segmentConfig = segmentConfig
      )

    //if mergeable is empty min gap requirement is 0 so that Segments get copied else
    //there should be at least of quarter or 100 key-values for gaps to be created.
    val minGapSize = if (mergeableCount == 0) 0 else (mergeableCount / 4) max 100

    val (mergeableHead, mergeableTail, future) =
      Segment.runOnGapsParallel[ListBuffer[TransientSegment.One]](
        headGap = headGap,
        tailGap = tailGap,
        empty = ListBuffer.empty[TransientSegment.One],
        minGapSize = minGapSize,
        segmentParallelism = segmentParallelism
      )(thunk = writeGap)

    if (mergeableCount > 0) {
      val midSegments =
        SegmentRefWriter.mergeOne(
          oldKeyValuesCount = ref.getKeyValueCount(),
          oldKeyValues = ref.iterator(),
          headGap = mergeableHead,
          tailGap = mergeableTail,
          mergeableCount = mergeableCount,
          mergeable = mergeable,
          removeDeletes = removeDeletes,
          createdInLevel = createdInLevel,
          valuesConfig = valuesConfig,
          sortedIndexConfig = sortedIndexConfig,
          binarySearchIndexConfig = binarySearchIndexConfig,
          hashIndexConfig = hashIndexConfig,
          bloomFilterConfig = bloomFilterConfig,
          segmentConfig = segmentConfig
        )

      val (headSegments, tailSegments) = Await.result(future, segmentParallelism.timeout)
      val newSegments = headSegments ++ midSegments ++ tailSegments
      SegmentMergeResult(result = newSegments, replaced = true)
    } else {
      val (headSegments, tailSegments) = Await.result(future, segmentParallelism.timeout)
      val newSegments = headSegments ++ tailSegments
      SegmentMergeResult(result = newSegments, replaced = false)
    }
  }

  def fastAssignAndMerge(headGap: Iterable[Assignable],
                         tailGap: Iterable[Assignable],
                         segmentRefs: Iterator[SegmentRef],
                         assignableCount: Int,
                         assignables: Iterator[Assignable],
                         removeDeletes: Boolean,
                         createdInLevel: Int,
                         segmentParallelism: SegmentParallelism,
                         valuesConfig: ValuesBlock.Config,
                         sortedIndexConfig: SortedIndexBlock.Config,
                         binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                         hashIndexConfig: HashIndexBlock.Config,
                         bloomFilterConfig: BloomFilterBlock.Config,
                         segmentConfig: SegmentBlock.Config)(implicit idGenerator: IDGenerator,
                                                             executionContext: ExecutionContext,
                                                             keyOrder: KeyOrder[Slice[Byte]],
                                                             timeOrder: TimeOrder[Slice[Byte]],
                                                             functionStore: FunctionStore): SegmentMergeResult[Slice[TransientSegment.SingletonOrMany]] = {
    if (assignableCount == 0) {
      //if there are no assignments write gaps and return.
      //      SegmentRef.writeRemoteSegmentAware(
      //        assignable = gap,
      //        removeDeletes = removeDeletes,
      //        createdInLevel = createdInLevel,
      //        valuesConfig = valuesConfig,
      //        sortedIndexConfig = sortedIndexConfig,
      //        binarySearchIndexConfig = binarySearchIndexConfig,
      //        hashIndexConfig = hashIndexConfig,
      //        bloomFilterConfig = bloomFilterConfig,
      //        segmentConfig = segmentConfig
      //      )
      //
      //      val gapInsert =
      //        SegmentRef.fastPutMany(
      //          assignables = Seq(headGap, tailGap),
      //          removeDeletes = removeDeletes,
      //          createdInLevel = createdInLevel,
      //          segmentParallelism = segmentParallelism,
      //          valuesConfig = valuesConfig,
      //          sortedIndexConfig = sortedIndexConfig,
      //          binarySearchIndexConfig = binarySearchIndexConfig,
      //          hashIndexConfig = hashIndexConfig,
      //          bloomFilterConfig = bloomFilterConfig,
      //          segmentConfig = segmentConfig
      //        ).get
      //
      //      if (gapInsert.isEmpty) {
      //        SegmentMergeResult(result = Slice.empty, replaced = false)
      //      } else {
      //        val gapSlice = Slice.of[TransientSegment.SingletonOrMany](gapInsert.foldLeft(0)(_ + _.size))
      //        gapInsert foreach gapSlice.addAll
      //        SegmentMergeResult(result = gapSlice, replaced = false)
      //      }
      ???
    } else {
      val (assignmentSegments, untouchedSegments) = segmentRefs.duplicate

      //assign key-values to Segment and then perform merge.
      val assignments =
        SegmentAssigner.assignUnsafeGapsSegmentRef[ListBuffer[Assignable]](
          assignablesCount = assignableCount,
          assignables = assignables,
          segments = assignmentSegments
        )

      if (assignments.isEmpty) {
        val exception = swaydb.Exception.MergeKeyValuesWithoutTargetSegment(assignableCount)
        val error = "Assigned segments are empty."
        logger.error(error, exception)
        throw exception
      } else {
        //keep oldRefs that are not assign and make sure they are added in order.

        def nextOldOrNull() = if (untouchedSegments.hasNext) untouchedSegments.next() else null

        val singles = ListBuffer.empty[TransientSegment.OneOrRemoteRef]

        if (headGap.nonEmpty)
          singles ++=
            fastMergeOne(
              assignables = headGap,
              createdInLevel = createdInLevel,
              valuesConfig = valuesConfig,
              sortedIndexConfig = sortedIndexConfig,
              binarySearchIndexConfig = binarySearchIndexConfig,
              hashIndexConfig = hashIndexConfig,
              bloomFilterConfig = bloomFilterConfig,
              segmentConfig = segmentConfig
            )

        assignments foreach {
          assignment =>

            var oldRef: SegmentRef = nextOldOrNull()

            //insert SegmentRefs directly that are not assigned or do not require merging making
            //sure they are inserted in order.
            while (oldRef != null && oldRef != assignment.segment) {
              singles += TransientSegment.RemoteRef(fileHeader = Slice.emptyBytes, ref = oldRef)

              oldRef = nextOldOrNull()
            }

            val result =
              SegmentRefWriter.fastMergeOne(
                ref = assignment.segment,
                headGap = assignment.headGap.result,
                tailGap = assignment.tailGap.result,
                mergeableCount = assignment.midOverlap.size,
                mergeable = assignment.midOverlap.iterator,
                removeDeletes = removeDeletes,
                createdInLevel = createdInLevel,
                segmentParallelism = segmentParallelism,
                valuesConfig = valuesConfig,
                sortedIndexConfig = sortedIndexConfig,
                binarySearchIndexConfig = binarySearchIndexConfig,
                hashIndexConfig = hashIndexConfig,
                bloomFilterConfig = bloomFilterConfig,
                segmentConfig = segmentConfig
              )

            if (result.replaced) {
              singles ++= result.result
            } else {
              val merge = result.result :+ TransientSegment.RemoteRef(fileHeader = Slice.emptyBytes, ref = assignment.segment)

              singles ++= merge.sortBy(_.minKey)(keyOrder)
            }
        }

        untouchedSegments foreach {
          oldRef =>
            singles += TransientSegment.RemoteRef(fileHeader = Slice.emptyBytes, ref = oldRef)
        }

        if (tailGap.nonEmpty)
          singles ++=
            fastMergeOne(
              assignables = tailGap,
              createdInLevel = createdInLevel,
              valuesConfig = valuesConfig,
              sortedIndexConfig = sortedIndexConfig,
              binarySearchIndexConfig = binarySearchIndexConfig,
              hashIndexConfig = hashIndexConfig,
              bloomFilterConfig = bloomFilterConfig,
              segmentConfig = segmentConfig
            )

        val newMany =
          SegmentBlock.writeOneOrMany(
            createdInLevel = createdInLevel,
            ones = Slice.from(singles, singles.size),
            sortedIndexConfig = sortedIndexConfig,
            hashIndexConfig = hashIndexConfig,
            binarySearchIndexConfig = binarySearchIndexConfig,
            valuesConfig = valuesConfig,
            segmentConfig = segmentConfig
          )

        SegmentMergeResult(result = newMany, replaced = true)
      }
    }
  }

  def refresh(ref: SegmentRef,
              removeDeletes: Boolean,
              createdInLevel: Int,
              valuesConfig: ValuesBlock.Config,
              sortedIndexConfig: SortedIndexBlock.Config,
              binarySearchIndexConfig: BinarySearchIndexBlock.Config,
              hashIndexConfig: HashIndexBlock.Config,
              bloomFilterConfig: BloomFilterBlock.Config,
              segmentConfig: SegmentBlock.Config)(implicit keyOrder: KeyOrder[Slice[Byte]]): Slice[TransientSegment.OneOrMany] = {
    //    val footer = ref.getFooter()
    val iterator = ref.iterator()
    //if it's created in the same level the required spaces for sortedIndex and values
    //will be the same as existing or less than the current sizes so there is no need to create a
    //MergeState builder.

    //NOTE - IGNORE created in same Level as configurations can change on boot-up.
    //    if (footer.createdInLevel == createdInLevel)
    //      Segment.refreshForSameLevel(
    //        sortedIndexBlock = ref.segmentBlockCache.getSortedIndex(),
    //        valuesBlock = ref.segmentBlockCache.getValues(),
    //        iterator = iterator,
    //        keyValuesCount = footer.keyValueCount,
    //        removeDeletes = removeDeletes,
    //        createdInLevel = createdInLevel,
    //        valuesConfig = valuesConfig,
    //        sortedIndexConfig = sortedIndexConfig,
    //        binarySearchIndexConfig = binarySearchIndexConfig,
    //        hashIndexConfig = hashIndexConfig,
    //        bloomFilterConfig = bloomFilterConfig,
    //        segmentConfig = segmentConfig
    //      )
    //    else
    Segment.refreshForNewLevel(
      keyValues = iterator,
      removeDeletes = removeDeletes,
      createdInLevel = createdInLevel,
      valuesConfig = valuesConfig,
      sortedIndexConfig = sortedIndexConfig,
      binarySearchIndexConfig = binarySearchIndexConfig,
      hashIndexConfig = hashIndexConfig,
      bloomFilterConfig = bloomFilterConfig,
      segmentConfig = segmentConfig
    )
  }

}
