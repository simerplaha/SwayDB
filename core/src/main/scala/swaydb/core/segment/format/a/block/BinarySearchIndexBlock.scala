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

import swaydb.Error.Segment.ErrorHandler
import swaydb.compression.CompressionInternal
import swaydb.core.data.{Persistent, Transient}
import swaydb.core.segment.format.a.block.reader.UnblockedReader
import swaydb.core.util.{Bytes, FunctionUtil, MinMax, Options}
import swaydb.data.config.{IOAction, IOStrategy, UncompressedBlockInfo}
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.data.util.ByteSizeOf
import swaydb.{Error, IO}

import scala.annotation.tailrec

private[core] object BinarySearchIndexBlock {

  val blockName = this.getClass.getSimpleName.dropRight(1)

  object Config {

    val disabled =
      Config(
        enabled = false,
        minimumNumberOfKeys = 0,
        fullIndex = false,
        searchSortedIndexDirectlyIfPreNormalised = true,
        blockIO = dataType => IOStrategy.SynchronisedIO(cacheOnAccess = dataType.isCompressed),
        compressions = _ => Seq.empty
      )

    def apply(config: swaydb.data.config.BinarySearchKeyIndex): Config =
      config match {
        case swaydb.data.config.BinarySearchKeyIndex.Disable(searchSortedIndexDirectly) =>
          Config(
            enabled = false,
            minimumNumberOfKeys = Int.MaxValue,
            fullIndex = false,
            searchSortedIndexDirectlyIfPreNormalised = searchSortedIndexDirectly,
            blockIO = dataType => IOStrategy.SynchronisedIO(cacheOnAccess = dataType.isCompressed),
            compressions = _ => Seq.empty
          )

        case enable: swaydb.data.config.BinarySearchKeyIndex.FullIndex =>
          Config(
            enabled = true,
            minimumNumberOfKeys = enable.minimumNumberOfKeys,
            searchSortedIndexDirectlyIfPreNormalised = enable.searchSortedIndexDirectly,
            fullIndex = true,
            blockIO = FunctionUtil.safe(IOStrategy.synchronisedStoredIfCompressed, enable.ioStrategy),
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
            searchSortedIndexDirectlyIfPreNormalised = enable.searchSortedIndexDirectlyIfPreNormalised,
            fullIndex = false,
            blockIO = FunctionUtil.safe(IOStrategy.synchronisedStoredIfCompressed, enable.ioStrategy),
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
                    searchSortedIndexDirectlyIfPreNormalised: Boolean,
                    fullIndex: Boolean,
                    blockIO: IOAction => IOStrategy,
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

  def init(normalisedKeyValues: Iterable[Transient],
           originalKeyValues: Iterable[Transient]): Option[State] =
    if (normalisedKeyValues.last.stats.segmentBinarySearchIndexSize <= 0 ||
      normalisedKeyValues.last.sortedIndexConfig.normaliseIndex ||
      (originalKeyValues.last.binarySearchIndexConfig.searchSortedIndexDirectlyIfPreNormalised && originalKeyValues.last.stats.hasSameIndexSizes()))
      None
    else
      BinarySearchIndexBlock.State(
        largestValue = normalisedKeyValues.last.stats.thisKeyValuesAccessIndexOffset,
        //not using size from stats because it's size does not account for hashIndex's missed keys.
        uniqueValuesCount = normalisedKeyValues.last.stats.segmentUniqueKeysCount,
        isFullIndex = normalisedKeyValues.last.binarySearchIndexConfig.fullIndex,
        minimumNumberOfKeys = normalisedKeyValues.last.binarySearchIndexConfig.minimumNumberOfKeys,
        compressions = normalisedKeyValues.last.binarySearchIndexConfig.compressions
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

  def close(state: State): IO[swaydb.Error.Segment, Option[State]] =
    if (state.bytes.isEmpty)
      IO.none
    else if (state.hasMinimumKeys)
      Block.block(
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

  def read(header: Block.Header[BinarySearchIndexBlock.Offset]): IO[swaydb.Error.Segment, BinarySearchIndexBlock] =
    for {
      valuesCount <- header.headerReader.readIntUnsigned()
      bytesPerValue <- header.headerReader.readInt()
      isFullIndex <- header.headerReader.readBoolean()
    } yield
      BinarySearchIndexBlock(
        offset = header.offset,
        valuesCount = valuesCount,
        headerSize = header.headerSize,
        bytesPerValue = bytesPerValue,
        isFullIndex = isFullIndex,
        compressionInfo = header.compressionInfo
      )

  def write(value: Int,
            state: State): IO[swaydb.Error.Segment, Unit] =
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

  def resolveResult(knownLowest: Option[Persistent],
                    knownMatch: Option[Persistent],
                    startKeyValue: Option[Persistent],
                    isHigherSeek: Option[Boolean])(implicit order: Ordering[Persistent]): IO[swaydb.Error.Segment, SearchResult[Persistent]] =
    knownMatch flatMap {
      knownMatch =>
        isHigherSeek map {
          isHigher =>
            //if higher got a successful match return the result with knowLost.
            if (isHigher)
              IO.Success(SearchResult.Some(knownLowest, knownMatch))
            else //if it's lower seek then match is the lower match.
              IO.Success(SearchResult.Some(None, knownMatch))
        }
    } getOrElse {
      //if there was not match create a response from known collected seeks.
      knownLowest flatMap {
        knowLowest =>
          isHigherSeek map {
            higher =>
              //if it was higher and knownMatch is none means there was no successful higher but lower might be know.
              if (higher)
                IO.Success(SearchResult.None(knownLowest))
              else //if it was lower then send the best known lower as the response.
                IO.Success(SearchResult.Some(None, knowLowest))
          }
      } getOrElse {
        //if no data return None response with lower set.
        val lowestMax = MinMax.maxFavourLeft(knownLowest, startKeyValue)
        IO.Success(SearchResult.None(lowestMax))
      }
    }

  def search(context: BinarySearchContext)(implicit ordering: KeyOrder[Slice[Byte]]): IO[swaydb.Error.Segment, SearchResult[Persistent]] = {
    implicit val order: Ordering[Persistent] = Ordering.by[Persistent, Slice[Byte]](_.key)(ordering)

    @tailrec
    def hop(start: Int, end: Int, knownLowest: Option[Persistent], knownMatch: Option[Persistent]): IO[swaydb.Error.Segment, SearchResult[Persistent]] = {
      val mid = start + (end - start) / 2

      val entryOffset = mid * context.entrySize

      if (start > end)
        resolveResult(
          knownLowest = knownLowest,
          knownMatch = knownMatch,
          startKeyValue = context.startKeyValue,
          isHigherSeek = context.higherOrLower
        )
      else
        context.seek(entryOffset) match {
          case IO.Success(entry) =>
            entry match {
              case matched: KeyMatcher.Result.Matched =>
                context.higherOrLower match {
                  case None =>
                    IO.Success(SearchResult.Some(matched.previous orElse knownLowest, matched.result))

                  case Some(higher) =>
                    if (higher)
                      if (matched.previous.isDefined)
                        IO.Success(SearchResult.Some(matched.previous, matched.result))
                      else
                        hop(start = start, end = mid - 1, knownLowest, knownMatch = Some(matched.result))
                    else if (matched.next.isDefined)
                    //Is lower! Don't need to compare knownLowest because a successful match would've fetch the nearest lowest.
                    //Here most times knownLowest would be the same as matched.result.
                      IO.Success(SearchResult.Some(matched.previous, matched.result))
                    else
                      hop(start = mid + 1, end = end, matched.previous orElse knownLowest, knownMatch = Some(matched.result))
                }

              case behind: KeyMatcher.Result.Behind =>
                //if the accessIndexPosition is not enabled then the start key could be lower than the seek key.
                //do a comparison of the highest of both (currently knownLowest and current seeked lowest)
                //seek can also return a lower lower if its not a fullIndex so do a check again.
                val newKnownLowest =
                  if (!context.isFullIndex || context.startKeyValue.exists(_.accessPosition == 0))
                    MinMax.maxFavourLeft(knownLowest, behind.previous)
                  else
                    behind.previous

                hop(start = mid + 1, end = end, Some(newKnownLowest), knownMatch = knownMatch)

              case KeyMatcher.Result.AheadOrNoneOrEnd =>
                hop(start = start, end = mid - 1, knownLowest, knownMatch = knownMatch)
            }
          case IO.Failure(error) =>
            IO.Failure(error)
        }
    }

    //accessPositions start from 1 but BinarySearch starts from 0.
    //A 0 accessPosition indicates that accessPositionIndex was disabled.
    //A key-values accessPosition can sometimes be larger than what binarySearchIndex knows for cases where binarySearchIndex is partial
    //to handle that check that accessPosition is not over the number total binarySearchIndex entries.
    def getAccessPosition(keyValue: Persistent): Option[Int] =
      if (keyValue.accessPosition <= 0 || (!context.isFullIndex && keyValue.accessPosition > context.entriesCount))
        None
      else
        Some(keyValue.accessPosition - 1)

    def getStartPosition(keyValue: Option[Persistent]): Int =
      keyValue
        .flatMap(getAccessPosition)
        .getOrElse(0)

    def getEndPosition(keyValue: Option[Persistent]): Int =
      keyValue
        .flatMap(getAccessPosition)
        .getOrElse(context.entriesCount - 1)

    val startPosition = getStartPosition(context.startKeyValue)
    val endPosition = getEndPosition(context.endKeyValue)

    /**
     * Special treatment for lower seeks.
     * If it's a lower seek test if the end-1 or start is the next lower else do full binary search.
     * Higher does not require special binary search seek because there is [[SortedIndexBlock.searchHigherSeekOne]]
     * but for lower cannot have lowerSeekOne since reads move forward and there need binary search to find the next
     * lowest index.
     */
    def seekLower() =
      if (context.endKeyValue.exists(_.accessPosition > 0))
        hop(start = (endPosition - 1) max 0, end = endPosition, None, None) flatMap {
          case some @ SearchResult.Some(_, _) =>
            IO.Success(some)

          case none @ SearchResult.None(lower) =>
            if (endPosition > 1)
              hop(start = startPosition, end = endPosition - 1, context.startKeyValue, None)
            else

            /**
             * [[seekLower]] returning a [[SearchResult.None]] with lower defined is not expected. If it does occur it
             * would be due to small number of entries in the binarySearch index (1 or 2) where startFrom is returned
             * to be lower. Here we check if lower is set and the assert is for development environment only.
             * If lower is returned the check should assert that it's the same object as original startFrom else return none.
             */
              lower flatMap {
                lower =>
                  context.startKeyValue map {
                    startKeyValue =>
                      if (lower.hashCode() != startKeyValue.hashCode())
                        IO.failed("Lower was defined.")
                      else
                        IO.Success(SearchResult.None(None))
                  }
              } getOrElse IO.Success(none)
        }
      else if (context.startKeyValue.exists(_.accessPosition > 0)) //end should not be larger than the number of entries.
        hop(start = startPosition, end = (startPosition + 1) min (context.entriesCount - 1), None, None) flatMap {
          case some @ SearchResult.Some(_, lower) =>
            if (context.startKeyValue exists (order.equiv(_, lower)))
              IO.Success(some)
            else
              hop(start = getStartPosition(Some(lower)), end = endPosition, Some(lower), None)

          case SearchResult.None(_) =>
            hop(start = startPosition, end = endPosition, context.startKeyValue, None)
        }
      else
        hop(start = startPosition, end = endPosition, context.startKeyValue, None)

    if (context.higherOrLower contains false)
      seekLower()
    else
      hop(start = startPosition, end = endPosition, context.startKeyValue, None)
  }

  def search(key: Slice[Byte],
             start: Option[Persistent],
             end: Option[Persistent],
             keyValuesCount: Int,
             sortedIndex: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
             values: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]])(implicit ordering: KeyOrder[Slice[Byte]]): IO[Error.Segment, Option[Persistent]] =
    search(
      BinarySearchContext(
        key = key,
        highOrLow = None,
        start = start,
        end = end,
        keyValuesCount = keyValuesCount,
        sortedIndex = sortedIndex,
        values = values
      )
    ).map(_.toOption)

  def search(key: Slice[Byte],
             start: Option[Persistent],
             end: Option[Persistent],
             binarySearchIndexReader: UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock],
             sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
             valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]])(implicit ordering: KeyOrder[Slice[Byte]]): IO[swaydb.Error.Segment, SearchResult[Persistent]] =
    search(
      BinarySearchContext(
        key = key,
        highOrLow = None,
        start = start,
        end = end,
        binarySearchIndex = binarySearchIndexReader,
        sortedIndex = sortedIndexReader,
        values = valuesReader
      )
    )

  def searchHigher(key: Slice[Byte],
                   start: Option[Persistent],
                   end: Option[Persistent],
                   binarySearchIndexReader: UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock],
                   sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                   valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]])(implicit ordering: KeyOrder[Slice[Byte]]): IO[swaydb.Error.Segment, SearchResult[Persistent]] =
    search(
      BinarySearchContext(
        key = key,
        highOrLow = Options.`true`,
        start = start,
        end = end,
        binarySearchIndex = binarySearchIndexReader,
        sortedIndex = sortedIndexReader,
        values = valuesReader
      )
    )

  def searchHigher(key: Slice[Byte],
                   start: Option[Persistent],
                   end: Option[Persistent],
                   keyValuesCount: Int,
                   sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                   valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]])(implicit ordering: KeyOrder[Slice[Byte]]): IO[Error.Segment, Option[Persistent]] =
    search(
      BinarySearchContext(
        key = key,
        highOrLow = Options.`true`,
        start = start,
        end = end,
        keyValuesCount = keyValuesCount,
        sortedIndex = sortedIndexReader,
        values = valuesReader
      )
    ).map(_.toOption)

  def searchLower(key: Slice[Byte],
                  start: Option[Persistent],
                  end: Option[Persistent],
                  binarySearchIndexReader: UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock],
                  sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                  valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]])(implicit ordering: KeyOrder[Slice[Byte]]): IO[swaydb.Error.Segment, SearchResult[Persistent]] =
    search(
      BinarySearchContext(
        key = key,
        highOrLow = Options.`false`,
        start = start,
        end = end,
        binarySearchIndex = binarySearchIndexReader,
        sortedIndex = sortedIndexReader,
        values = valuesReader
      )
    )

  def searchLower(key: Slice[Byte],
                  start: Option[Persistent],
                  end: Option[Persistent],
                  keyValuesCount: Int,
                  sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                  valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]])(implicit ordering: KeyOrder[Slice[Byte]]): IO[Error.Segment, Option[Persistent]] =
    search(
      BinarySearchContext(
        key = key,
        highOrLow = Options.`false`,
        start = start,
        end = end,
        keyValuesCount = keyValuesCount,
        sortedIndex = sortedIndexReader,
        values = valuesReader
      )
    ).map(_.toOption)

  implicit object BinarySearchIndexBlockOps extends BlockOps[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock] {
    override def updateBlockOffset(block: BinarySearchIndexBlock, start: Int, size: Int): BinarySearchIndexBlock =
      block.copy(offset = BinarySearchIndexBlock.Offset(start = start, size = size))

    override def createOffset(start: Int, size: Int): Offset =
      BinarySearchIndexBlock.Offset(start, size)

    override def readBlock(header: Block.Header[Offset]): IO[swaydb.Error.Segment, BinarySearchIndexBlock] =
      BinarySearchIndexBlock.read(header)
  }

}

private[core] case class BinarySearchIndexBlock(offset: BinarySearchIndexBlock.Offset,
                                                valuesCount: Int,
                                                headerSize: Int,
                                                bytesPerValue: Int,
                                                isFullIndex: Boolean,
                                                compressionInfo: Option[Block.CompressionInfo]) extends Block[BinarySearchIndexBlock.Offset] {
  val isVarInt: Boolean =
    BinarySearchIndexBlock.isVarInt(bytesPerValue)
}
