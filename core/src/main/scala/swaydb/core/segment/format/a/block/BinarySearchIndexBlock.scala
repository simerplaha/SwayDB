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

import swaydb.Error.Segment.ExceptionHandler
import swaydb.IO
import swaydb.compression.CompressionInternal
import swaydb.core.data.Persistent.Partial
import swaydb.core.data.{Persistent, Transient}
import swaydb.core.segment.format.a.block.reader.UnblockedReader
import swaydb.core.util.{Bytes, MinMax, Options}
import swaydb.data.config.{IOAction, IOStrategy, UncompressedBlockInfo}
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.data.util.{ByteSizeOf, Functions}

import scala.annotation.tailrec

private[core] object BinarySearchIndexBlock {

  val blockName = this.getClass.getSimpleName.dropRight(1)

  object Config {

    val disabled =
      Config(
        enabled = false,
        minimumNumberOfKeys = 0,
        fullIndex = false,
        searchSortedIndexDirectlyIfPossible = true,
        blockIO = dataType => IOStrategy.SynchronisedIO(cacheOnAccess = dataType.isCompressed),
        compressions = _ => Seq.empty
      )

    def apply(config: swaydb.data.config.BinarySearchIndex): Config =
      config match {
        case swaydb.data.config.BinarySearchIndex.Disable(searchSortedIndexDirectly) =>
          Config(
            enabled = false,
            minimumNumberOfKeys = Int.MaxValue,
            fullIndex = false,
            searchSortedIndexDirectlyIfPossible = searchSortedIndexDirectly,
            blockIO = dataType => IOStrategy.SynchronisedIO(cacheOnAccess = dataType.isCompressed),
            compressions = _ => Seq.empty
          )

        case enable: swaydb.data.config.BinarySearchIndex.FullIndex =>
          Config(
            enabled = true,
            minimumNumberOfKeys = enable.minimumNumberOfKeys,
            searchSortedIndexDirectlyIfPossible = enable.searchSortedIndexDirectly,
            fullIndex = true,
            blockIO = Functions.safe(IOStrategy.synchronisedStoredIfCompressed, enable.ioStrategy),
            compressions =
              Functions.safe(
                default = _ => Seq.empty[CompressionInternal],
                function = enable.compression(_) map CompressionInternal.apply
              )
          )

        case enable: swaydb.data.config.BinarySearchIndex.SecondaryIndex =>
          Config(
            enabled = true,
            minimumNumberOfKeys = enable.minimumNumberOfKeys,
            searchSortedIndexDirectlyIfPossible = enable.searchSortedIndexDirectlyIfPreNormalised,
            fullIndex = false,
            blockIO = Functions.safe(IOStrategy.synchronisedStoredIfCompressed, enable.ioStrategy),
            compressions =
              Functions.safe(
                default = _ => Seq.empty[CompressionInternal],
                function = enable.compression(_) map CompressionInternal.apply
              )
          )
      }
  }

  case class Config(enabled: Boolean,
                    minimumNumberOfKeys: Int,
                    searchSortedIndexDirectlyIfPossible: Boolean,
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
           originalKeyValues: Iterable[Transient]): Option[State] = {
    val normalisedLast = normalisedKeyValues.last

    if (normalisedLast.stats.segmentBinarySearchIndexSize <= 0 ||
      normalisedLast.sortedIndexConfig.normaliseIndex ||
      (originalKeyValues.last.binarySearchIndexConfig.searchSortedIndexDirectlyIfPossible && !originalKeyValues.last.stats.hasPrefixCompression && originalKeyValues.last.stats.hasSameIndexSizes()))
      None
    else
      BinarySearchIndexBlock.State(
        largestValue = normalisedLast.stats.thisKeyValuesAccessIndexOffset,
        //not using size from stats because it's size does not account for hashIndex's missed keys.
        uniqueValuesCount = normalisedLast.stats.segmentUniqueKeysCount,
        isFullIndex = normalisedLast.binarySearchIndexConfig.fullIndex,
        minimumNumberOfKeys = normalisedLast.binarySearchIndexConfig.minimumNumberOfKeys,
        compressions = normalisedLast.binarySearchIndexConfig.compressions
      )
  }

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

  def search(context: BinarySearchContext)(implicit ordering: KeyOrder[Slice[Byte]]): IO[swaydb.Error.Segment, SearchResult[Persistent.Partial]] = {
    implicit val order: Ordering[Persistent.Partial] = Ordering.by[Persistent.Partial, Slice[Byte]](_.key)(ordering)

    @tailrec
    def hop(start: Int, end: Int, knownLowest: Option[Persistent.Partial], knownMatch: Option[Persistent.Partial]): IO[swaydb.Error.Segment, SearchResult[Persistent.Partial]] = {
      val mid = start + (end - start) / 2

      println(s"start: $start, mid: $mid, end: $end")

      val valueOffset = mid * context.bytesPerValue

      if (start > end)
        IO.Right {
          SearchResult.None(
            MinMax.maxFavourLeft(
              left = knownLowest,
              right = context.startKeyValue
            )
          )
        }
      else
        context.seek(valueOffset) match {
          case IO.Right(entry) =>
            entry match {
              case matched: KeyMatcher.Result.Matched =>
                IO.Right {
                  SearchResult.Some(
                    lower =
                      MinMax.maxFavourLeft(
                        left = knownLowest orElse context.startKeyValue,
                        right = matched.previous orElse context.startKeyValue
                      ),
                    value = matched.result
                  )
                }

              case behind: KeyMatcher.Result.Behind =>
                hop(start = mid + 1, end = end, knownLowest = Some(behind.previous), knownMatch = knownMatch)

              case KeyMatcher.Result.AheadOrNoneOrEnd =>
                hop(start = start, end = mid - 1, knownLowest = knownLowest, knownMatch = knownMatch)
            }

          case IO.Left(error) =>
            IO.Left(error)
        }
    }

    //accessPositions start from 1 but BinarySearch starts from 0.
    //A 0 accessPosition indicates that accessPositionIndex was disabled.
    //A key-values accessPosition can sometimes be larger than what binarySearchIndex knows for cases where binarySearchIndex is partial
    //to handle that check that accessPosition is not over the number total binarySearchIndex entries.
    def getAccessPosition(keyValue: Persistent.Partial): Option[Int] =
      if (keyValue.accessPosition <= 0 || (!context.isFullIndex && keyValue.accessPosition > context.valuesCount))
        None
      else
        Some(keyValue.accessPosition - 1)

    def getStartPosition(keyValue: Option[Persistent.Partial]): Int =
      keyValue
        .flatMap(getAccessPosition)
        .getOrElse(0)

    def getEndPosition(keyValue: Option[Persistent.Partial]): Int =
      keyValue
        .flatMap(getAccessPosition)
        .getOrElse(context.valuesCount - 1)

    val startPosition = getStartPosition(context.startKeyValue)
    val endPosition = getEndPosition(context.endKeyValue)

    //println(s"startKey: ${context.startKeyValue.map(_.key.readInt())}, endKey: ${context.endKeyValue.map(_.key.readInt())}")

    hop(start = startPosition, end = endPosition, context.startKeyValue, None)
  }

  def search(key: Slice[Byte],
             start: Option[Persistent.Partial],
             end: Option[Persistent.Partial],
             keyValuesCount: => IO[swaydb.Error.Segment, Int],
             binarySearchIndexReader: Option[UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock]],
             sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
             valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]])(implicit ordering: KeyOrder[Slice[Byte]]): IO[swaydb.Error.Segment, SearchResult[Persistent.Partial]] =
    if (sortedIndexReader.block.isBinarySearchable)
      keyValuesCount flatMap {
        keyValuesCount =>
          search(
            BinarySearchContext(
              key = key,
              start = start,
              end = end,
              keyValuesCount = keyValuesCount,
              sortedIndex = sortedIndexReader,
              values = valuesReader
            )
          )
      }
    else
      binarySearchIndexReader map {
        binarySearchIndexReader =>
          search(
            BinarySearchContext(
              key = key,
              start = start,
              end = end,
              binarySearchIndex = binarySearchIndexReader,
              sortedIndex = sortedIndexReader,
              values = valuesReader
            )
          ) flatMap {
            case some: SearchResult.Some[Persistent.Partial] =>
              IO.Right(some)

            case none @ SearchResult.None(lower) =>
              if (binarySearchIndexReader.block.isFullIndex && !sortedIndexReader.block.hasPrefixCompression)
                IO.Right(none)
              else
                lower.map(_.toPersistent.map(Some(_))).getOrElse(IO.none) flatMap {
                  lower =>
                    SortedIndexBlock.search(
                      key = key,
                      startFrom = lower,
                      fullRead = true,
                      sortedIndexReader = sortedIndexReader,
                      valuesReader = valuesReader
                    ) flatMap {
                      case Some(value) =>
                        IO.Right(SearchResult.Some(lower, value))

                      case None =>
                        IO.Right(SearchResult.None(lower))
                    }
                }
          }
      } getOrElse SearchResult.noneIO

  def searchHigher(key: Slice[Byte],
                   start: Option[Persistent.Partial],
                   end: Option[Persistent.Partial],
                   keyValuesCount: => IO[swaydb.Error.Segment, Int],
                   binarySearchIndexReader: Option[UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock]],
                   sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                   valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]])(implicit ordering: KeyOrder[Slice[Byte]]): IO[swaydb.Error.Segment, SearchResult[Persistent.Partial]] =
    search(
      key = key,
      start = start,
      end = end,
      keyValuesCount = keyValuesCount,
      binarySearchIndexReader = binarySearchIndexReader,
      sortedIndexReader = sortedIndexReader,
      valuesReader = valuesReader
    ) flatMap {
      case none @ SearchResult.None(lower) =>
        lower match {
          case Some(lower) =>
            lower.toPersistent flatMap {
              lower =>
                val someLower = Some(lower)
                SortedIndexBlock.searchHigher(
                  key = key,
                  startFrom = someLower,
                  fullRead = true,
                  sortedIndexReader = sortedIndexReader,
                  valuesReader = valuesReader
                ) map {
                  case Some(value) =>
                    SearchResult.Some(someLower, value)

                  case None =>
                    SearchResult.None(someLower)
                }
            }

          case None =>
            IO.Right(none)
        }

      case result @ SearchResult.Some(_, lower) =>
        lower match {
          case lower: Partial.Fixed =>
            if (lower.nextIndexSize <= 0)
              IO.Right(SearchResult.None(Some(lower)))
            else
              lower.toPersistent flatMap {
                got =>
                  val someLower = Some(got)
                  SortedIndexBlock.searchHigher(
                    key = key,
                    startFrom = someLower,
                    fullRead = true,
                    sortedIndexReader = sortedIndexReader,
                    valuesReader = valuesReader
                  ) map {
                    case Some(higher) =>
                      SearchResult.Some(someLower, higher)

                    case None =>
                      SearchResult.None(someLower)
                  }
              }

          case _: Partial.RangeT =>
            IO.Right(result)

          case _: Partial.GroupT =>
            IO.Right(result)
        }
    }

  def searchLower(key: Slice[Byte],
                  start: Option[Persistent.Partial],
                  end: Option[Persistent.Partial],
                  keyValuesCount: => IO[swaydb.Error.Segment, Int],
                  binarySearchIndexReader: Option[UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock]],
                  sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                  valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]])(implicit ordering: KeyOrder[Slice[Byte]]): IO[swaydb.Error.Segment, SearchResult[Persistent.Partial]] =
    search(
      key = key,
      start = start,
      end = end,
      keyValuesCount = keyValuesCount,
      binarySearchIndexReader = binarySearchIndexReader,
      sortedIndexReader = sortedIndexReader,
      valuesReader = valuesReader
    ) map {
      case SearchResult.None(lower) =>
        lower match {
          case Some(lower) =>
            SearchResult.Some(None, lower)

          case None =>
            SearchResult.none
        }

      case SearchResult.Some(lower, _) =>
        lower match {
          case Some(lower) =>
            SearchResult.Some(None, lower)

          case None =>
            SearchResult.none
        }
    }

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
