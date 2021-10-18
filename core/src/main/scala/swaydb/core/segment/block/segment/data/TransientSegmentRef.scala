/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb.core.segment.block.segment.data

import swaydb.core.segment.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.block.bloomfilter.BloomFilterBlock
import swaydb.core.segment.block.hashindex.HashIndexBlock
import swaydb.core.segment.block.reader.UnblockedReader
import swaydb.core.segment.block.segment.footer.SegmentFooterBlock
import swaydb.core.segment.block.sortedindex.SortedIndexBlock
import swaydb.core.segment.block.values.ValuesBlock
import swaydb.core.util.MinMax
import swaydb.data.MaxKey
import swaydb.data.slice.Slice

import scala.concurrent.duration.Deadline

class TransientSegmentRef(val minKey: Slice[Byte],
                          val maxKey: MaxKey[Slice[Byte]],
                          val functionMinMax: Option[MinMax[Slice[Byte]]],
                          val nearestDeadline: Option[Deadline],
                          val createdInLevel: Int,
                          //counts
                          val updateCount: Int,
                          val rangeCount: Int,
                          val putCount: Int,
                          val putDeadlineCount: Int,
                          val keyValueCount: Int,
                          //values
                          val valuesBlockHeader: Option[Slice[Byte]],
                          val valuesBlock: Option[Slice[Byte]],
                          val valuesUnblockedReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
                          //sortedIndex
                          val sortedIndexClosedState: SortedIndexBlock.State,
                          val sortedIndexBlockHeader: Slice[Byte],
                          val sortedIndexBlock: Slice[Byte],
                          val sortedIndexUnblockedReader: Option[UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock]],
                          //hashIndex
                          val hashIndexBlockHeader: Option[Slice[Byte]],
                          val hashIndexBlock: Option[Slice[Byte]],
                          val hashIndexUnblockedReader: Option[UnblockedReader[HashIndexBlock.Offset, HashIndexBlock]],
                          //binarySearch
                          val binarySearchIndexBlockHeader: Option[Slice[Byte]],
                          val binarySearchIndexBlock: Option[Slice[Byte]],
                          val binarySearchUnblockedReader: Option[UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock]],
                          //bloomFilter
                          val bloomFilterBlockHeader: Option[Slice[Byte]],
                          val bloomFilterBlock: Option[Slice[Byte]],
                          val bloomFilterUnblockedReader: Option[UnblockedReader[BloomFilterBlock.Offset, BloomFilterBlock]],
                          //footer
                          val footerBlock: Slice[Byte]) {

  val segmentHeader: Slice[Byte] = Slice.of[Byte](Byte.MaxValue)

  val segmentBytes: Slice[Slice[Byte]] = {
    val allBytes = Slice.of[Slice[Byte]](13)
    allBytes add segmentHeader

    valuesBlockHeader foreach allBytes.add
    valuesBlock foreach allBytes.add

    allBytes add sortedIndexBlockHeader
    allBytes add sortedIndexBlock

    hashIndexBlockHeader foreach allBytes.add
    hashIndexBlock foreach allBytes.add

    binarySearchIndexBlockHeader foreach allBytes.add
    binarySearchIndexBlock foreach allBytes.add

    bloomFilterBlockHeader foreach allBytes.add
    bloomFilterBlock foreach allBytes.add

    allBytes add footerBlock
  }

  //If sortedIndexUnblockedReader is defined then caching is enabled
  //so read footer block.

  val footerUnblocked: Option[SegmentFooterBlock] =
    if (sortedIndexUnblockedReader.isDefined)
      Some(
        SegmentFooterBlock.readCRCPassed(
          footerStartOffset = 0,
          footerSize = footerBlock.size,
          footerBytes = footerBlock
        )
      )
    else
      None

  def isEmpty: Boolean =
    segmentBytes.exists(_.isEmpty)

  def segmentSize =
    segmentBytes.foldLeft(0)(_ + _.size)

  def flattenSegmentBytes: Slice[Byte] = {
    val size = segmentBytes.foldLeft(0)(_ + _.size)
    val slice = Slice.of[Byte](size)
    segmentBytes foreach (slice addAll _)
    assert(slice.isFull)
    slice
  }
}
