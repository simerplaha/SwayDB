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

package swaydb.core.segment.block.binarysearch

import swaydb.config.UncompressedBlockInfo
import swaydb.core.compression.CoreCompression
import swaydb.core.segment.block.binarysearch.BinarySearchIndexBlock.optimalBytesRequired
import swaydb.slice.{Slice, SliceMut}
import swaydb.utils.{Maybe, SomeOrNone}

sealed trait BinarySearchIndexBlockStateOption extends SomeOrNone[BinarySearchIndexBlockStateOption, BinarySearchIndexBlockState] {
  override def noneS: BinarySearchIndexBlockStateOption =
    BinarySearchIndexBlockState.Null
}

private[block] case object BinarySearchIndexBlockState {

  final case object Null extends BinarySearchIndexBlockStateOption {
    override def isNoneS: Boolean = true

    override def getS: BinarySearchIndexBlockState = throw new Exception(s"${BinarySearchIndexBlockState.productPrefix} is of type ${Null.productPrefix}")
  }

  def apply(format: BinarySearchEntryFormat,
            largestIndexOffset: Int,
            largestMergedKeySize: Int,
            uniqueValuesCount: Int,
            isFullIndex: Boolean,
            minimumNumberOfKeys: Int,
            compressions: UncompressedBlockInfo => Iterable[CoreCompression]): BinarySearchIndexBlockStateOption =
    if (uniqueValuesCount < minimumNumberOfKeys) {
      BinarySearchIndexBlockState.Null
    } else {
      val bytesPerValue =
        format.bytesToAllocatePerEntry(
          largestIndexOffset = largestIndexOffset,
          largestMergedKeySize = largestMergedKeySize
        )

      val bytesRequired: Int =
        optimalBytesRequired(
          largestIndexOffset = largestIndexOffset,
          largestMergedKeySize = largestMergedKeySize,
          valuesCount = uniqueValuesCount,
          minimNumberOfKeysForBinarySearchIndex = minimumNumberOfKeys,
          bytesToAllocatedPerEntryMaybe = Maybe.some(bytesPerValue),
          format = format
        )

      val bytes = Slice.allocate[Byte](bytesRequired)

      new BinarySearchIndexBlockState(
        format = format,
        bytesPerValue = bytesPerValue,
        uniqueValuesCount = uniqueValuesCount,
        _previousWritten = Int.MinValue,
        writtenValues = 0,
        minimumNumberOfKeys = minimumNumberOfKeys,
        isFullIndex = isFullIndex,
        compressibleBytes = bytes,
        cacheableBytes = bytes,
        header = null,
        compressions = compressions
      )
    }
}

private[block] class BinarySearchIndexBlockState(val format: BinarySearchEntryFormat,
                                                 val bytesPerValue: Int,
                                                 val uniqueValuesCount: Int,
                                                 var _previousWritten: Int,
                                                 var writtenValues: Int,
                                                 val minimumNumberOfKeys: Int,
                                                 var isFullIndex: Boolean,
                                                 var compressibleBytes: SliceMut[Byte],
                                                 val cacheableBytes: Slice[Byte],
                                                 var header: Slice[Byte],
                                                 val compressions: UncompressedBlockInfo => Iterable[CoreCompression]) extends BinarySearchIndexBlockStateOption {

  override def isNoneS: Boolean =
    false

  override def getS: BinarySearchIndexBlockState =
    this

  def blockSize: Int =
    header.size + compressibleBytes.size

  def incrementWrittenValuesCount(): Unit =
    writtenValues += 1

  def previouslyWritten_=(previouslyWritten: Int): Unit =
    this._previousWritten = previouslyWritten

  def previouslyWritten: Int =
    _previousWritten

  def hasMinimumKeys: Boolean =
    writtenValues >= minimumNumberOfKeys
}
