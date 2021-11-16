package swaydb.core.segment.block.binarysearch

import swaydb.compression.CompressionInternal
import swaydb.core.segment.block.binarysearch.BinarySearchIndexBlock.optimalBytesRequired
import swaydb.data.config.UncompressedBlockInfo
import swaydb.slice.{Slice, SliceMut}
import swaydb.utils.Maybe

private[block] object BinarySearchIndexBlockState {

  def apply(format: BinarySearchEntryFormat,
            largestIndexOffset: Int,
            largestMergedKeySize: Int,
            uniqueValuesCount: Int,
            isFullIndex: Boolean,
            minimumNumberOfKeys: Int,
            compressions: UncompressedBlockInfo => Iterable[CompressionInternal]): Option[BinarySearchIndexBlockState] =
    if (uniqueValuesCount < minimumNumberOfKeys) {
      None
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

      val bytes = Slice.of[Byte](bytesRequired)

      val state =
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

      Some(state)
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
                                                 val compressions: UncompressedBlockInfo => Iterable[CompressionInternal]) {

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
