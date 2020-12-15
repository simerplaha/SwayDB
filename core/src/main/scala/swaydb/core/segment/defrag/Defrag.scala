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

import swaydb.core.data.Memory
import swaydb.core.function.FunctionStore
import swaydb.core.merge.MergeStats
import swaydb.core.segment.SegmentSource
import swaydb.core.segment.SegmentSource._
import swaydb.core.segment.assigner.Assignable
import swaydb.core.segment.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.block.bloomfilter.BloomFilterBlock
import swaydb.core.segment.block.hashindex.HashIndexBlock
import swaydb.core.segment.block.segment.SegmentBlock
import swaydb.core.segment.block.segment.data.TransientSegment
import swaydb.core.segment.block.sortedindex.SortedIndexBlock
import swaydb.core.segment.block.values.ValuesBlock
import swaydb.core.segment.ref.SegmentMergeResult
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.util.Futures
import swaydb.data.util.Futures._

import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}

object Defrag {

  def run[A, B >: A](source: Option[A],
                     nullSource: B,
                     fragments: ListBuffer[TransientSegment.Fragment],
                     headGap: ListBuffer[Either[MergeStats.Persistent.Builder[Memory, ListBuffer], Assignable.Collection]],
                     tailGap: ListBuffer[Either[MergeStats.Persistent.Builder[Memory, ListBuffer], Assignable.Collection]],
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
                                                         functionStore: FunctionStore,
                                                         segmentSource: SegmentSource[A]): Future[SegmentMergeResult[B, ListBuffer[TransientSegment.Fragment]]] =
    Futures
      .unit
      .flatMapUnit {
        if (headGap.isEmpty)
          Future.successful(fragments)
        else
          Future {
            DefragGap.run(
              gap = headGap,
              fragments = fragments,
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
      .flatMap {
        fragments =>
          source match {
            case Some(source) =>
              //forceExpand if there are cleanable key-values or if segment size is too small.
              val forceExpand =
                (removeDeletes && source.hasNonPut) || ((headGap.nonEmpty || tailGap.nonEmpty) && source.segmentSize < segmentConfig.minSize && source.getKeyValueCount() < segmentConfig.maxCount)

              DefragMerge.run(
                source = source,
                nullSource = nullSource,
                mergeableCount = mergeableCount,
                mergeable = mergeable,
                removeDeletes = removeDeletes,
                forceExpand = forceExpand,
                fragments = fragments
              ) map {
                source =>
                  //if there was no segment replacement then create a fence so head and tail gaps do not get collapsed.
                  if (source == nullSource)
                    fragments += TransientSegment.Fence

                  SegmentMergeResult(
                    source = source,
                    result = fragments
                  )
              }

            case None =>
              //create a fence so tail does not get collapsed into head.
              fragments += TransientSegment.Fence

              val result =
                SegmentMergeResult(
                  source = nullSource,
                  result = fragments
                )

              Future.successful(result)
          }

      }
      .flatMap {
        mergeResult =>
          if (tailGap.isEmpty)
            Future.successful(mergeResult)
          else
            Future {
              DefragGap.run(
                gap = tailGap,
                fragments = mergeResult.result,
                removeDeletes = removeDeletes,
                createdInLevel = createdInLevel,
                valuesConfig = valuesConfig,
                sortedIndexConfig = sortedIndexConfig,
                binarySearchIndexConfig = binarySearchIndexConfig,
                hashIndexConfig = hashIndexConfig,
                bloomFilterConfig = bloomFilterConfig,
                segmentConfig = segmentConfig
              )

              mergeResult
            }
      }

}
