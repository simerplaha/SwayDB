/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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
 */

package swaydb.core.segment.format.a.block

import swaydb.compression.CompressionInternal
import swaydb.core.data.{Persistent, Transient}
import swaydb.core.segment.format.a.block.reader.DecompressedBlockReader
import swaydb.core.util.{Bytes, FunctionUtil, Options}
import swaydb.data.IO
import swaydb.data.config.{BlockIO, BlockStatus, UncompressedBlockInfo}
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.data.util.ByteSizeOf

import scala.annotation.tailrec

private[core] object BinarySearchIndexBlock {

  val blockName = this.getClass.getSimpleName.dropRight(1)

  object Config {

    val disabled =
      Config(
        enabled = false,
        minimumNumberOfKeys = 0,
        fullIndex = false,
        blockIO = blockStatus => BlockIO.SynchronisedIO(cacheOnAccess = blockStatus.isCompressed),
        compressions = _ => Seq.empty
      )

    def apply(config: swaydb.data.config.BinarySearchKeyIndex): Config =
      config match {
        case swaydb.data.config.BinarySearchKeyIndex.Disable =>
          Config(
            enabled = false,
            minimumNumberOfKeys = Int.MaxValue,
            fullIndex = false,
            blockIO = blockStatus => BlockIO.SynchronisedIO(cacheOnAccess = blockStatus.isCompressed),
            compressions = _ => Seq.empty
          )

        case enable: swaydb.data.config.BinarySearchKeyIndex.FullIndex =>
          Config(
            enabled = true,
            minimumNumberOfKeys = enable.minimumNumberOfKeys,
            fullIndex = true,
            blockIO = FunctionUtil.safe(BlockIO.default, enable.blockIO),
            compressions =
              FunctionUtil.safe(
                default = _ => Seq.empty[CompressionInternal],
                function = enable.compression(_) map CompressionInternal.apply
              )
          )

        case enable: swaydb.data.config.BinarySearchKeyIndex.SecondaryIndex =>
          Config(
            enabled = true,
            minimumNumberOfKeys = enable.minimumNumberOfKeys,
            fullIndex = false,
            blockIO = FunctionUtil.safe(BlockIO.default, enable.blockIO),
            compressions =
              FunctionUtil.safe(
                default = _ => Seq.empty[CompressionInternal],
                function = enable.compression(_) map CompressionInternal.apply
              )
          )
      }
  }

  case class Config(enabled: Boolean,
                    minimumNumberOfKeys: Int,
                    fullIndex: Boolean,
                    blockIO: BlockStatus => BlockIO,
                    compressions: UncompressedBlockInfo => Seq[CompressionInternal])

  case class Offset(start: Int, size: Int) extends BlockOffset

  object State {
    def apply(largestValue: Int,
              uniqueValuesCount: Int,
              isFullIndex: Boolean,
              minimumNumberOfKeys: Int,
              compressions: UncompressedBlockInfo => Seq[CompressionInternal]): Option[State] =
      if (uniqueValuesCount < minimumNumberOfKeys) {
        None
      } else {
        val headerSize: Int =
          optimalHeaderSize(
            largestValue = largestValue,
            valuesCount = uniqueValuesCount,
            hasCompression = true
          )
        val bytes: Int =
          optimalBytesRequired(
            largestValue = largestValue,
            valuesCount = uniqueValuesCount,
            hasCompression = true,
            minimNumberOfKeysForBinarySearchIndex = minimumNumberOfKeys
          )
        Some(
          new State(
            bytesPerValue = bytesToAllocatePerValue(largestValue),
            uniqueValuesCount = uniqueValuesCount,
            _previousWritten = Int.MinValue,
            writtenValues = 0,
            minimumNumberOfKeys = minimumNumberOfKeys,
            headerSize = headerSize,
            isFullIndex = isFullIndex,
            _bytes = Slice.create[Byte](bytes),
            compressions = compressions
          )
        )
      }
  }

  class State(val bytesPerValue: Int,
              val uniqueValuesCount: Int,
              var _previousWritten: Int,
              var writtenValues: Int,
              val minimumNumberOfKeys: Int,
              val headerSize: Int,
              val isFullIndex: Boolean,
              var _bytes: Slice[Byte],
              val compressions: UncompressedBlockInfo => Seq[CompressionInternal]) {

    def incrementWrittenValuesCount() =
      writtenValues += 1

    def previouslyWritten_=(previouslyWritten: Int) =
      this._previousWritten = previouslyWritten

    def previouslyWritten = _previousWritten

    def bytes = _bytes

    def bytes_=(bytes: Slice[Byte]) =
      this._bytes = bytes

    def hasMinimumKeys =
      writtenValues >= minimumNumberOfKeys
  }

  def init(keyValues: Iterable[Transient]): Option[State] =
    if (keyValues.last.stats.segmentBinarySearchIndexSize <= 0)
      None
    else
      BinarySearchIndexBlock.State(
        largestValue = keyValues.last.stats.thisKeyValuesAccessIndexOffset,
        //not using size from stats because it's size does not account for hashIndex's missed keys.
        uniqueValuesCount = keyValues.last.stats.segmentUniqueKeysCount,
        isFullIndex = keyValues.last.binarySearchIndexConfig.fullIndex,
        minimumNumberOfKeys = keyValues.last.binarySearchIndexConfig.minimumNumberOfKeys,
        compressions = keyValues.last.binarySearchIndexConfig.compressions
      )

  def isVarInt(varIntSizeOfLargestValue: Int) =
    varIntSizeOfLargestValue < ByteSizeOf.int

  def bytesToAllocatePerValue(largestValue: Int): Int = {
    val varintSizeOfLargestValue = Bytes.sizeOf(largestValue)
    if (isVarInt(varintSizeOfLargestValue))
      varintSizeOfLargestValue
    else
      ByteSizeOf.int
  }

  def optimalBytesRequired(largestValue: Int,
                           valuesCount: Int,
                           hasCompression: Boolean,
                           minimNumberOfKeysForBinarySearchIndex: Int): Int =
    if (valuesCount < minimNumberOfKeysForBinarySearchIndex)
      0
    else
      optimalHeaderSize(
        largestValue = largestValue,
        valuesCount = valuesCount,
        hasCompression = hasCompression
      ) + (bytesToAllocatePerValue(largestValue) * valuesCount)

  def optimalHeaderSize(largestValue: Int,
                        valuesCount: Int,
                        hasCompression: Boolean): Int = {

    val headerSize =
      Block.headerSize(hasCompression) +
        Bytes.sizeOf(valuesCount) + //uniqueValuesCount
        ByteSizeOf.varInt + //bytesPerValue
        ByteSizeOf.boolean //isFullIndex

    Bytes.sizeOf(headerSize) +
      headerSize
  }

  def close(state: State): IO[Option[State]] =
    if (state.bytes.isEmpty)
      IO.none
    else if (state.hasMinimumKeys)
      Block.compress(
        headerSize = state.headerSize,
        bytes = state.bytes,
        compressions = state.compressions(UncompressedBlockInfo(state.bytes.size)),
        blockName = blockName
      ) flatMap {
        compressedOrUncompressedBytes =>
          IO {
            state.bytes = compressedOrUncompressedBytes
            state.bytes addIntUnsigned state.writtenValues
            state.bytes addInt state.bytesPerValue
            state.bytes addBoolean state.isFullIndex
            if (state.bytes.currentWritePosition > state.headerSize)
              throw new Exception(s"Calculated header size was incorrect. Expected: ${state.headerSize}. Used: ${state.bytes.currentWritePosition - 1}")
            Some(state)
          }
      }
    else
      IO.none

  def read(offset: Offset,
           reader: DecompressedBlockReader[SegmentBlock]): IO[BinarySearchIndexBlock] =
    for {
      result <- Block.readHeader(offset = offset, reader = reader)
      valuesCount <- result.headerReader.readIntUnsigned()
      bytesPerValue <- result.headerReader.readInt()
      isFullIndex <- result.headerReader.readBoolean()
    } yield
      BinarySearchIndexBlock(
        offset = offset,
        valuesCount = valuesCount,
        headerSize = result.headerSize,
        bytesPerValue = bytesPerValue,
        isFullIndex = isFullIndex,
        compressionInfo = result.compressionInfo
      )

  def write(value: Int,
            state: State): IO[Unit] =
    if (value == state.previouslyWritten) { //do not write duplicate entries.
      IO.unit
    } else
      IO {
        if (state.bytes.size == 0) state.bytes moveWritePosition state.headerSize
        //if the size of largest value is less than 4 bytes, write them as unsigned.
        if (state.bytesPerValue < ByteSizeOf.int) {
          val writePosition = state.bytes.currentWritePosition
          state.bytes addIntUnsigned value
          val missedBytes = state.bytesPerValue - (state.bytes.currentWritePosition - writePosition)
          if (missedBytes > 0)
            state.bytes moveWritePosition (state.bytes.currentWritePosition + missedBytes) //fill in the missing bytes to maintain fixed size for each entry.
        } else {
          state.bytes addInt value
        }

        state.incrementWrittenValuesCount()
        state.previouslyWritten = value
      }

  def search(reader: DecompressedBlockReader[BinarySearchIndexBlock],
             start: Option[Int],
             end: Option[Int],
             higherOrLower: Option[Boolean],
             matchValue: Int => IO[KeyMatcher.Result]) = {

    @tailrec
    def hop(start: Int, end: Int, lastKnown: Option[Persistent]): IO[Option[Persistent]] = {
      val mid = start + (end - start) / 2

      val valueOffset = mid * reader.block.bytesPerValue
      if (start > end)
        if (lastKnown.isDefined)
          IO.Success(lastKnown)
        else
          IO.none
      else {
        val value =
          if (reader.block.isVarInt)
            reader.moveTo(valueOffset).readIntUnsigned()
          else
            reader.moveTo(valueOffset).readInt()

        value.flatMap(matchValue) match {
          case IO.Success(value) =>
            value match {
              case matched: KeyMatcher.Result.Matched =>
                higherOrLower match {
                  case None =>
                    IO.Success(Some(matched.result))

                  case Some(higher) =>
                    if (higher) {
                      if (matched.previous.isDefined)
                        IO.Success(Some(matched.result))
                      else
                        hop(start = start, end = mid - 1, lastKnown = Some(matched.result))
                    } else {
                      //is lower
                      if (matched.next.isDefined)
                        IO.Success(Some(matched.result))
                      else
                        hop(start = mid + 1, end = end, lastKnown = Some(matched.result))
                    }
                }

              case KeyMatcher.Result.BehindFetchNext | KeyMatcher.Result.BehindStopped =>
                hop(start = mid + 1, end = end, lastKnown = lastKnown)

              case KeyMatcher.Result.AheadOrNoneOrEnd =>
                hop(start = start, end = mid - 1, lastKnown = lastKnown)
            }
          case IO.Failure(error) =>
            IO.Failure(error)
        }
      }
    }

    hop(start = start.getOrElse(0), end = end.getOrElse(reader.block.valuesCount - 1), None)
  }

  private def search(key: Slice[Byte],
                     higherOrLower: Option[Boolean],
                     start: Option[Persistent],
                     end: Option[Persistent],
                     binarySearchIndex: DecompressedBlockReader[BinarySearchIndexBlock],
                     sortedIndex: DecompressedBlockReader[SortedIndexBlock],
                     values: Option[DecompressedBlockReader[ValuesBlock]])(implicit ordering: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] = {
    val matcher =
      higherOrLower map {
        higher =>
          //if the sortedIndex has compression disabled do not fetch the next key-value. Let binary search find the next one to seek to.
          if (higher)
            if (sortedIndex.block.hasPrefixCompression)
              KeyMatcher.Higher.WhilePrefixCompressed(key)
            else
              KeyMatcher.Higher.MatchOnly(key)
          else if (sortedIndex.block.hasPrefixCompression)
            KeyMatcher.Lower.WhilePrefixCompressed(key)
          else
            KeyMatcher.Lower.MatchOnly(key)
      } getOrElse {
        if (sortedIndex.block.hasPrefixCompression)
          KeyMatcher.Get.WhilePrefixCompressed(key)
        else
          KeyMatcher.Get.MatchOnly(key)
      }

    search(
      reader = binarySearchIndex,
      higherOrLower = higherOrLower,
      start =
        start flatMap {
          keyValue =>
            if (keyValue.accessPosition <= 0)
              None
            else
              Some(keyValue.accessPosition - 1)
        },
      end =
        end flatMap {
          keyValue =>
            if (keyValue.accessPosition <= 0)
              None
            else
              Some(keyValue.accessPosition - 1)
        },
      matchValue =
        sortedIndexOffsetValue =>
          SortedIndexBlock.findAndMatchOrNextMatch(
            matcher = matcher,
            fromOffset = sortedIndexOffsetValue,
            sortedIndex = sortedIndex,
            values = values
          )
    )
  }

  def search(key: Slice[Byte],
             start: Option[Persistent],
             end: Option[Persistent],
             binarySearchIndexReader: DecompressedBlockReader[BinarySearchIndexBlock],
             sortedIndexReader: DecompressedBlockReader[SortedIndexBlock],
             valuesReader: Option[DecompressedBlockReader[ValuesBlock]])(implicit ordering: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] =
    search(
      key = key,
      higherOrLower = None,
      start = start,
      end = end,
      binarySearchIndex = binarySearchIndexReader,
      sortedIndex = sortedIndexReader,
      values = valuesReader
    )

  def searchHigher(key: Slice[Byte],
                   start: Option[Persistent],
                   end: Option[Persistent],
                   binarySearchIndexReader: DecompressedBlockReader[BinarySearchIndexBlock],
                   sortedIndexReader: DecompressedBlockReader[SortedIndexBlock],
                   valuesReader: Option[DecompressedBlockReader[ValuesBlock]])(implicit ordering: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] =
    search(
      key = key,
      higherOrLower = Options.`true`,
      start = start,
      end = end,
      binarySearchIndex = binarySearchIndexReader,
      sortedIndex = sortedIndexReader,
      values = valuesReader
    )

  def searchLower(key: Slice[Byte],
                  start: Option[Persistent],
                  end: Option[Persistent],
                  binarySearchIndexReader: DecompressedBlockReader[BinarySearchIndexBlock],
                  sortedIndexReader: DecompressedBlockReader[SortedIndexBlock],
                  valuesReader: Option[DecompressedBlockReader[ValuesBlock]])(implicit ordering: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] =
    search(
      key = key,
      higherOrLower = Options.`false`,
      start = start,
      end = end,
      binarySearchIndex = binarySearchIndexReader,
      sortedIndex = sortedIndexReader,
      values = valuesReader
    )

  implicit object BinarySearchIndexBlockUpdater extends BlockUpdater[BinarySearchIndexBlock] {
    override def updateOffset(block: BinarySearchIndexBlock, start: Int, size: Int): BinarySearchIndexBlock =
      block.copy(offset = BinarySearchIndexBlock.Offset(start = start, size = size))
  }

}

private[core] case class BinarySearchIndexBlock(offset: BinarySearchIndexBlock.Offset,
                                                valuesCount: Int,
                                                headerSize: Int,
                                                bytesPerValue: Int,
                                                isFullIndex: Boolean,
                                                compressionInfo: Option[Block.CompressionInfo]) extends Block {
  val isVarInt: Boolean =
    BinarySearchIndexBlock.isVarInt(bytesPerValue)
}
