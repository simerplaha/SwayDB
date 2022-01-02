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

package swaydb.core.segment.block.sortedindex

import swaydb.core.compression.CoreCompression
import swaydb.config.UncompressedBlockInfo
import swaydb.core.segment.data.Memory
import swaydb.core.segment.entry.writer.EntryWriter
import swaydb.core.util.MinMax
import swaydb.slice.{MaxKey, Slice, SliceMut}
import swaydb.utils.SomeOrNone

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Deadline

sealed trait SortedIndexBlockStateOption extends SomeOrNone[SortedIndexBlockStateOption, SortedIndexBlockState] {
  override def noneS: SortedIndexBlockStateOption =
    SortedIndexBlockState.Null
}

case object SortedIndexBlockState {

  final case object Null extends SortedIndexBlockStateOption {
    override def isNoneS: Boolean = true

    override def getS: SortedIndexBlockState = throw new Exception(s"${SortedIndexBlockState.productPrefix} is of type ${Null.productPrefix}")
  }
}

/**
 * [[SortedIndexBlockState]] is mostly mutable because these vars calculate runtime stats of
 * a Segment which is used to build other blocks. Immutable version of this
 * resulted in very slow compaction because immutable compaction was creation
 * millions of temporary objects every second causing GC halts.
 */
private[block] class SortedIndexBlockState(var compressibleBytes: SliceMut[Byte],
                                           var cacheableBytes: Slice[Byte],
                                           var header: Slice[Byte],
                                           var minKey: Slice[Byte],
                                           var maxKey: MaxKey[Slice[Byte]],
                                           var lastKeyValue: Memory,
                                           var smallestIndexEntrySize: Int,
                                           var largestIndexEntrySize: Int,
                                           var largestMergedKeySize: Int,
                                           var largestUncompressedMergedKeySize: Int,
                                           val enablePrefixCompression: Boolean,
                                           var entriesCount: Int,
                                           var prefixCompressedCount: Int,
                                           val shouldPrefixCompress: Int => Boolean,
                                           var nearestDeadline: Option[Deadline],
                                           var rangeCount: Int,
                                           var updateCount: Int,
                                           var putCount: Int,
                                           var putDeadlineCount: Int,
                                           var mightContainRemoveRange: Boolean,
                                           var minMaxFunctionId: Option[MinMax[Slice[Byte]]],
                                           val enableAccessPositionIndex: Boolean,
                                           var optimiseForReverseIteration: Boolean,
                                           val compressDuplicateRangeValues: Boolean,
                                           val normaliseIndex: Boolean,
                                           val compressions: UncompressedBlockInfo => Iterable[CoreCompression],
                                           val secondaryIndexEntries: ListBuffer[SortedIndexBlockSecondaryIndexEntry],
                                           val indexEntries: ListBuffer[Slice[Byte]],
                                           val builder: EntryWriter.Builder) extends SortedIndexBlockStateOption {

  override def isNoneS: Boolean =
    false

  override def getS: SortedIndexBlockState =
    this

  def blockBytes: Slice[Byte] =
    header ++ compressibleBytes

  def uncompressedPrefixCount: Int =
    entriesCount - prefixCompressedCount

  def hasPrefixCompression: Boolean =
    builder.segmentHasPrefixCompression

  def prefixCompressKeysOnly =
    builder.prefixCompressKeysOnly

  def isPreNormalised: Boolean =
    hasSameIndexSizes()

  def hasSameIndexSizes(): Boolean =
    smallestIndexEntrySize == largestIndexEntrySize

  def blockSize: Int =
    header.size + compressibleBytes.size

}
