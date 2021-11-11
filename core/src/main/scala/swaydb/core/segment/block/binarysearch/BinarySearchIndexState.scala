package swaydb.core.segment.block.binarysearch

import swaydb.compression.CompressionInternal
import swaydb.core.segment.block.binarysearch.BinarySearchIndexBlock.optimalBytesRequired
import swaydb.data.config.UncompressedBlockInfo
import swaydb.data.slice.Slice
import swaydb.utils.Maybe

object BinarySearchIndexState {

  def apply(format: BinarySearchEntryFormat,
            largestIndexOffset: Int,
            largestMergedKeySize: Int,
            uniqueValuesCount: Int,
            isFullIndex: Boolean,
            minimumNumberOfKeys: Int,
            compressions: UncompressedBlockInfo => Iterable[CompressionInternal]): Option[BinarySearchIndexState] =
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
        new BinarySearchIndexState(
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

class BinarySearchIndexState(val format: BinarySearchEntryFormat,
                             val bytesPerValue: Int,
                             val uniqueValuesCount: Int,
                             var _previousWritten: Int,
                             var writtenValues: Int,
                             val minimumNumberOfKeys: Int,
                             var isFullIndex: Boolean,
                             var compressibleBytes: Slice[Byte],
                             val cacheableBytes: Slice[Byte],
                             var header: Slice[Byte],
                             val compressions: UncompressedBlockInfo => Iterable[CompressionInternal]) {

  def blockSize: Int =
    header.size + compressibleBytes.size

  def incrementWrittenValuesCount() =
    writtenValues += 1

  def previouslyWritten_=(previouslyWritten: Int) =
    this._previousWritten = previouslyWritten

  def previouslyWritten = _previousWritten

  def hasMinimumKeys =
    writtenValues >= minimumNumberOfKeys
}
