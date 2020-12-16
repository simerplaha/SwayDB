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
import swaydb.core.data.Memory
import swaydb.core.function.FunctionStore
import swaydb.core.merge.MergeStats
import swaydb.core.segment.Segment
import swaydb.core.segment.assigner.{Assignable, GapAggregator, SegmentAssigner}
import swaydb.core.segment.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.block.bloomfilter.BloomFilterBlock
import swaydb.core.segment.block.hashindex.HashIndexBlock
import swaydb.core.segment.block.segment.SegmentBlock
import swaydb.core.segment.block.segment.data.TransientSegment
import swaydb.core.segment.block.sortedindex.SortedIndexBlock
import swaydb.core.segment.block.values.ValuesBlock
import swaydb.core.segment.defrag.Defrag
import swaydb.core.util.IDGenerator
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.util.Futures
import swaydb.data.util.Futures._

import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}

private[segment] object SegmentRefWriter extends LazyLogging {

  def run(ref: SegmentRef,
          headGap: ListBuffer[Assignable.Gap[MergeStats.Persistent.Builder[Memory, ListBuffer]]],
          tailGap: ListBuffer[Assignable.Gap[MergeStats.Persistent.Builder[Memory, ListBuffer]]],
          mergeableCount: Int,
          mergeable: Iterator[Assignable],
          removeDeletes: Boolean,
          createdInLevel: Int,
          valuesConfig: ValuesBlock.Config,
          sortedIndexConfig: SortedIndexBlock.Config,
          binarySearchIndexConfig: BinarySearchIndexBlock.Config,
          hashIndexConfig: HashIndexBlock.Config,
          bloomFilterConfig: BloomFilterBlock.Config,
          segmentConfig: SegmentBlock.Config)(implicit executionContext: ExecutionContext,
                                              keyOrder: KeyOrder[Slice[Byte]],
                                              timeOrder: TimeOrder[Slice[Byte]],
                                              functionStore: FunctionStore): Future[SegmentMergeResult[SegmentRefOption, Slice[TransientSegment.Persistent]]] =
    Defrag.run(
      source = Some(ref),
      nullSource = SegmentRef.Null,
      fragments = ListBuffer.empty,
      headGap = headGap,
      tailGap = tailGap,
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
    ) flatMap {
      mergeResult =>
        //        writeTransient(
        //          resultBuffer = mergeResult.result,
        //          removeDeletes = removeDeletes,
        //          createdInLevel = createdInLevel,
        //          valuesConfig = valuesConfig,
        //          sortedIndexConfig = sortedIndexConfig,
        //          binarySearchIndexConfig = binarySearchIndexConfig,
        //          hashIndexConfig = hashIndexConfig,
        //          bloomFilterConfig = bloomFilterConfig,
        //          segmentConfig = segmentConfig
        //        ) map {
        //          transientSegments =>
        //            mergeResult map {
        //              _ =>
        //                transientSegments
        //            }
        //        }
        ???
    }

  def run(headGap: ListBuffer[Assignable.Gap[MergeStats.Persistent.Builder[Memory, ListBuffer]]],
          tailGap: ListBuffer[Assignable.Gap[MergeStats.Persistent.Builder[Memory, ListBuffer]]],
          segmentRefs: => Iterator[SegmentRef],
          assignableCount: Int,
          assignables: Iterator[Assignable],
          removeDeletes: Boolean,
          createdInLevel: Int,
          valuesConfig: ValuesBlock.Config,
          sortedIndexConfig: SortedIndexBlock.Config,
          binarySearchIndexConfig: BinarySearchIndexBlock.Config,
          hashIndexConfig: HashIndexBlock.Config,
          bloomFilterConfig: BloomFilterBlock.Config,
          segmentConfig: SegmentBlock.Config)(implicit idGenerator: IDGenerator,
                                              executionContext: ExecutionContext,
                                              keyOrder: KeyOrder[Slice[Byte]],
                                              timeOrder: TimeOrder[Slice[Byte]],
                                              functionStore: FunctionStore): Future[SegmentMergeResult[SegmentRefOption, Slice[TransientSegment.Persistent]]] =
    if (assignableCount == 0)
      Defrag.run(
        source = Option.empty[SegmentRef],
        nullSource = SegmentRef.Null,
        fragments = ListBuffer.empty,
        headGap = headGap,
        tailGap = tailGap,
        mergeableCount = 0,
        mergeable = Assignable.emptyIterator,
        removeDeletes = removeDeletes,
        createdInLevel = createdInLevel,
        valuesConfig = valuesConfig,
        sortedIndexConfig = sortedIndexConfig,
        binarySearchIndexConfig = binarySearchIndexConfig,
        hashIndexConfig = hashIndexConfig,
        bloomFilterConfig = bloomFilterConfig,
        segmentConfig = segmentConfig
      ) flatMap {
        mergeResult =>
          //          writeTransient(
          //            resultBuffer = mergeResult.result,
          //            removeDeletes = removeDeletes,
          //            createdInLevel = createdInLevel,
          //            valuesConfig = valuesConfig,
          //            sortedIndexConfig = sortedIndexConfig,
          //            binarySearchIndexConfig = binarySearchIndexConfig,
          //            hashIndexConfig = hashIndexConfig,
          //            bloomFilterConfig = bloomFilterConfig,
          //            segmentConfig = segmentConfig
          //          ) map {
          //            transientSegments =>
          //              mergeResult map {
          //                _ =>
          //                  transientSegments
          //              }
          //          }
          ???
      }
    else
    //      Futures
    //        .unit
    //        .flatMapUnit {
    //          if (headGap.isEmpty)
    //            Future.successful(ListBuffer.empty[Either[MergeStats.Persistent.Builder[Memory, ListBuffer], TransientSegment.Remote]])
    //          else
    //            Future {
    //              DefragGap.defrag(
    //                gap = headGap,
    //                resultBuffer = ListBuffer.empty,
    //                removeDeletes = removeDeletes,
    //                createdInLevel = createdInLevel,
    //                valuesConfig = valuesConfig,
    //                sortedIndexConfig = sortedIndexConfig,
    //                binarySearchIndexConfig = binarySearchIndexConfig,
    //                hashIndexConfig = hashIndexConfig,
    //                bloomFilterConfig = bloomFilterConfig,
    //                segmentConfig = segmentConfig
    //              )
    //            }
    //        }
    //        .flatMap {
    //          resultBuffer =>
    //            Future {
    //              val (assignmentRefs, untouchedRefs) = segmentRefs.duplicate
    //              implicit val creator = GapAggregator.persistentCreator(removeDeletes)
    //
    //              //assign key-values to Segment and then perform merge.
    //              val assignments =
    //                SegmentAssigner.assignUnsafeGapsSegmentRef[ListBuffer[Assignable.Assignment[MergeStats.Persistent.Builder[Memory, ListBuffer]]]](
    //                  assignablesCount = assignableCount,
    //                  assignables = assignables,
    //                  segments = assignmentRefs
    //                )
    //
    //              (assignments, resultBuffer, untouchedRefs)
    //            }
    //        }
    //        .flatMap {
    //          case (assignments, resultBuffer, untouchedRefs) =>
    //            Future.traverse(assignments) {
    //              assignment =>
    //                DefragSegmentRef.defrag(
    //                  source = assignment.segment,
    //                  resultBuffer = ListBuffer.empty,
    //                  headGap = assignment.headGap.result,
    //                  tailGap = assignment.tailGap.result,
    //                  mergeableCount = assignment.midOverlap.size,
    //                  mergeable = assignment.midOverlap.iterator,
    //                  removeDeletes = removeDeletes,
    //                  createdInLevel = createdInLevel,
    //                  valuesConfig = valuesConfig,
    //                  sortedIndexConfig = sortedIndexConfig,
    //                  binarySearchIndexConfig = binarySearchIndexConfig,
    //                  hashIndexConfig = hashIndexConfig,
    //                  bloomFilterConfig = bloomFilterConfig,
    //                  segmentConfig = segmentConfig
    //                )
    //            } map {
    //              result =>
    //                (result, untouchedRefs)
    //            }
    //        }
    //        .flatMap {
    //          case (mergeResults, untouchedRefs) =>
    //            mergeResults
    //            //            untouchedRefs.filter(untouched => mergeResults.exists(_.se))
    //            ???
    //        }
    //        .flatMap {
    //          //          case tuple @ (resultBuffer, replaced) =>
    //          //            if (tailGap.isEmpty)
    //          //              Future.successful(tuple)
    //          //            else
    //          //              Future {
    //          //                val buffer =
    //          //                  DefragGap.defrag(
    //          //                    gap = tailGap,
    //          //                    resultBuffer = resultBuffer,
    //          //                    removeDeletes = removeDeletes,
    //          //                    createdInLevel = createdInLevel,
    //          //                    valuesConfig = valuesConfig,
    //          //                    sortedIndexConfig = sortedIndexConfig,
    //          //                    binarySearchIndexConfig = binarySearchIndexConfig,
    //          //                    hashIndexConfig = hashIndexConfig,
    //          //                    bloomFilterConfig = bloomFilterConfig,
    //          //                    segmentConfig = segmentConfig
    //          //                  )
    //          //
    //          //                (buffer, replaced)
    //          //              }
    //          ???
    //        }
    //        .flatMap {
    //          //          case (resultBuffer, replaced) =>
    //          //            writeTransient(
    //          //              resultBuffer = resultBuffer,
    //          //              removeDeletes = removeDeletes,
    //          //              createdInLevel = createdInLevel,
    //          //              valuesConfig = valuesConfig,
    //          //              sortedIndexConfig = sortedIndexConfig,
    //          //              binarySearchIndexConfig = binarySearchIndexConfig,
    //          //              hashIndexConfig = hashIndexConfig,
    //          //              bloomFilterConfig = bloomFilterConfig,
    //          //              segmentConfig = segmentConfig
    //          //            ) map {
    //          //              transientSegments =>
    //          //                SegmentMergeResult(
    //          //                  result = transientSegments,
    //          //                  replaced = replaced
    //          //                )
    //          //            }
    //          ???
    //        }
      ???


  def refresh(ref: SegmentRef,
              removeDeletes: Boolean,
              createdInLevel: Int,
              valuesConfig: ValuesBlock.Config,
              sortedIndexConfig: SortedIndexBlock.Config,
              binarySearchIndexConfig: BinarySearchIndexBlock.Config,
              hashIndexConfig: HashIndexBlock.Config,
              bloomFilterConfig: BloomFilterBlock.Config,
              segmentConfig: SegmentBlock.Config)(implicit keyOrder: KeyOrder[Slice[Byte]]): Slice[TransientSegment.OneOrRemoteRefOrMany] = {
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

  private[ref] def writeTransient(resultBuffer: ListBuffer[Either[MergeStats.Persistent.Builder[Memory, ListBuffer], TransientSegment.Remote]],
                                  removeDeletes: Boolean,
                                  createdInLevel: Int,
                                  valuesConfig: ValuesBlock.Config,
                                  sortedIndexConfig: SortedIndexBlock.Config,
                                  binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                                  hashIndexConfig: HashIndexBlock.Config,
                                  bloomFilterConfig: BloomFilterBlock.Config,
                                  segmentConfig: SegmentBlock.Config)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                      ec: ExecutionContext): Future[Slice[TransientSegment.Persistent]] =
    Future.traverse(resultBuffer) {
      case Left(stats) =>
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

      case Right(segment) =>
        Future.successful(Slice(segment))
    } map {
      buffer =>
        //TODO - group SegmentRefs and write them as PersistentSegmentMany if needed.
        val slice = Slice.of[TransientSegment.Persistent](buffer.foldLeft(0)(_ + _.size))
        buffer foreach slice.addAll
        slice
    }
}
