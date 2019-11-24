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

package swaydb.core.segment.format.a.block.binarysearch

import swaydb.IO
import swaydb.compression.CompressionInternal
import swaydb.core.data.{Persistent, Transient}
import swaydb.core.segment.format.a.block._
import swaydb.core.segment.format.a.block.reader.UnblockedReader
import swaydb.core.util.Options._
import swaydb.core.util.{Bytes, MinMax}
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
        ioStrategy = dataType => IOStrategy.SynchronisedIO(cacheOnAccess = dataType.isCompressed),
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
            ioStrategy = dataType => IOStrategy.SynchronisedIO(cacheOnAccess = dataType.isCompressed),
            compressions = _ => Seq.empty
          )

        case enable: swaydb.data.config.BinarySearchIndex.FullIndex =>
          Config(
            enabled = true,
            minimumNumberOfKeys = enable.minimumNumberOfKeys,
            searchSortedIndexDirectlyIfPossible = enable.searchSortedIndexDirectly,
            fullIndex = true,
            ioStrategy = Functions.safe(IOStrategy.synchronisedStoredIfCompressed, enable.ioStrategy),
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
            ioStrategy = Functions.safe(IOStrategy.synchronisedStoredIfCompressed, enable.ioStrategy),
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
                    ioStrategy: IOAction => IOStrategy,
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
        uniqueValuesCount = normalisedLast.stats.uncompressedKeyCounts,
        isFullIndex = normalisedLast.binarySearchIndexConfig.fullIndex,
        minimumNumberOfKeys = normalisedLast.binarySearchIndexConfig.minimumNumberOfKeys,
        compressions = normalisedLast.binarySearchIndexConfig.compressions
      )
  }

  def isUnsignedInt(varIntSizeOfLargestValue: Int) =
    varIntSizeOfLargestValue < ByteSizeOf.int

  def bytesToAllocatePerValue(largestValue: Int): Int = {
    val varintSizeOfLargestValue = Bytes.sizeOfUnsignedInt(largestValue)
    if (isUnsignedInt(varintSizeOfLargestValue))
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
        Bytes.sizeOfUnsignedInt(valuesCount) + //uniqueValuesCount
        ByteSizeOf.varInt + //bytesPerValue
        ByteSizeOf.boolean //isFullIndex

    Bytes.sizeOfUnsignedInt(headerSize) +
      headerSize
  }

  def close(state: State): Option[State] =
    if (state.bytes.isEmpty)
      None
    else if (state.hasMinimumKeys) {
      val compressedOrUncompressedBytes =
        Block.block(
          headerSize = state.headerSize,
          bytes = state.bytes,
          compressions = state.compressions(UncompressedBlockInfo(state.bytes.size)),
          blockName = blockName
        )

      state.bytes = compressedOrUncompressedBytes
      state.bytes addUnsignedInt state.writtenValues
      state.bytes addInt state.bytesPerValue
      state.bytes addBoolean state.isFullIndex
      if (state.bytes.currentWritePosition > state.headerSize)
        throw IO.throwable(s"Calculated header size was incorrect. Expected: ${state.headerSize}. Used: ${state.bytes.currentWritePosition - 1}")
      Some(state)
    }
    else
      None

  def read(header: Block.Header[BinarySearchIndexBlock.Offset]): BinarySearchIndexBlock = {
    val valuesCount = header.headerReader.readUnsignedInt()
    val bytesPerValue = header.headerReader.readInt()
    val isFullIndex = header.headerReader.readBoolean()
    BinarySearchIndexBlock(
      offset = header.offset,
      valuesCount = valuesCount,
      headerSize = header.headerSize,
      bytesPerValue = bytesPerValue,
      isFullIndex = isFullIndex,
      compressionInfo = header.compressionInfo
    )
  }

  def write(value: Int,
            state: State): Unit =
    if (value == state.previouslyWritten) { //do not write duplicate entries.
      ()
    } else {
      if (state.bytes.size == 0) state.bytes moveWritePosition state.headerSize
      //if the size of largest value is less than 4 bytes, write them as unsigned.
      if (state.bytesPerValue < ByteSizeOf.int) {
        val writePosition = state.bytes.currentWritePosition
        state.bytes addUnsignedInt value
        val missedBytes = state.bytesPerValue - (state.bytes.currentWritePosition - writePosition)
        if (missedBytes > 0)
          state.bytes moveWritePosition (state.bytes.currentWritePosition + missedBytes) //fill in the missing bytes to maintain fixed size for each entry.
      } else {
        state.bytes addInt value
      }

      state.incrementWrittenValuesCount()
      state.previouslyWritten = value
    }

  //sortedIndexAccessPositions start from 1 but BinarySearch starts from 0.
  //A 0 sortedIndexAccessPosition indicates that sortedIndexAccessPositionIndex was disabled.
  //A key-values sortedIndexAccessPosition can sometimes be larger than what binarySearchIndex knows for cases where binarySearchIndex is partial
  //to handle that check that sortedIndexAccessPosition is not over the number total binarySearchIndex entries.
  def getSortedIndexAccessPosition(keyValue: Persistent, context: BinarySearchContext): Option[Int] =
    if (keyValue.sortedIndexAccessPosition <= 0 || (!context.isFullIndex && keyValue.sortedIndexAccessPosition > context.valuesCount))
      None
    else
      Some(keyValue.sortedIndexAccessPosition - 1)

  def getStartPosition(context: BinarySearchContext): Int =
    context.lowestKeyValue match {
      case Some(lowestKeyValue) =>
        getSortedIndexAccessPosition(lowestKeyValue, context) getOrElse 0

      case None =>
        0
    }

  def getEndPosition(context: BinarySearchContext): Int =
    context.highestKeyValue match {
      case Some(highestKeyValue) =>
        getSortedIndexAccessPosition(highestKeyValue, context) getOrElse (context.valuesCount - 1)

      case None =>
        context.valuesCount - 1
    }

  //  var totalHops = 0
  //  var maxHop = 0
  //  var minHop = 0
  //  var currentHops = 0
  //  var binarySeeks = 0
  //  var binarySuccessfulSeeks = 0
  //  var binaryFailedSeeks = 0
  //  var failedWithLower = 0
  //  var sameLower = 0
  //  var greaterLower = 0

  private[block] def binarySearch(context: BinarySearchContext)(implicit order: KeyOrder[Persistent]): BinarySearchGetResult[Persistent] = {

    @tailrec
    def hop(start: Int, end: Int, knownLowest: Option[Persistent], knownMatch: Option[Persistent]): BinarySearchGetResult[Persistent] = {
      val mid = start + (end - start) / 2

      //println(s"start: $start, mid: $mid, end: $end")

      //      totalHops += 1
      //      currentHops += 1

      val valueOffset = mid * context.bytesPerValue

      if (start > end)
        new BinarySearchGetResult.None(
          MinMax.maxFavourLeft(
            left = knownLowest,
            right = context.lowestKeyValue
          )
        )
      else
        context.seek(valueOffset) match {
          case matched: KeyMatcher.Result.Matched =>
            new BinarySearchGetResult.Some(value = matched.result)

          case behind: KeyMatcher.Result.Behind =>
            hop(start = mid + 1, end = end, knownLowest = Some(behind.previous), knownMatch = knownMatch)

          case _: KeyMatcher.Result.AheadOrNoneOrEnd =>
            hop(start = start, end = mid - 1, knownLowest = knownLowest, knownMatch = knownMatch)
        }
    }

    val start = getStartPosition(context)
    val end = getEndPosition(context)

    //println(s"lowestKey: ${context.lowestKeyValue.map(_.key.readInt())}, highestKey: ${context.highestKeyValue.map(_.key.readInt())}")
    hop(start = start, end = end, context.lowestKeyValue, None)
  }

  private def binarySearchLower(fetchLeft: Boolean, context: BinarySearchContext)(implicit ordering: KeyOrder[Slice[Byte]],
                                                                                  partialOrdering: KeyOrder[Persistent]): BinarySearchLowerResult.Some[Persistent] = {

    @tailrec
    def hop(start: Int, end: Int, knownLowest: Option[Persistent], knownMatch: Option[Persistent]): BinarySearchLowerResult.Some[Persistent] = {
      val mid = start + (end - start) / 2

      //println(s"start: $start, mid: $mid, end: $end, fetchLeft: $fetchLeft")

      val valueOffset = mid * context.bytesPerValue

      /**
       * if shifting left did not result in a valid lower key-value then reboot binarySearch without shift.
       * This can only occur if [[BinarySearchIndexBlock.isFullIndex]] is false || [[SortedIndexBlock.enableAccessPositionIndex]] is false.
       *
       * @example keys = [1 ... 30]
       *          binaryEntries = [1, 5, 10, 20]
       *          search key = 15
       *          end hint = 30
       *          shiftLeft will result in 20 which is not the lowest.
       */
      if (start > end || mid < 0)
        if (fetchLeft && knownLowest.isEmpty) {
          //println("Restart")
          binarySearchLower(fetchLeft = false, context = context)
        } else {
          //println("End")
          BinarySearchLowerResult.Some(
            lower =
              MinMax.maxFavourLeft(
                left = knownLowest,
                right = context.lowestKeyValue
              ),
            matched = knownMatch
          )
        }
      else
        context.seek(valueOffset) match {
          case matched: KeyMatcher.Result.Matched =>
            matched.result match {
              case fixed: Persistent.Fixed =>
                hop(start = mid - 1, end = mid - 1, knownLowest = matched.previous orElse knownLowest, knownMatch = Some(fixed))

              case range: Persistent.Range =>
                if (ordering.gt(context.targetKey, range.fromKey))
                  BinarySearchLowerResult.Some(
                    lower = None,
                    matched = Some(range)
                  )
                else
                  hop(start = mid - 1, end = mid - 1, knownLowest = matched.previous orElse knownLowest, knownMatch = Some(matched.result))
            }

          case behind: KeyMatcher.Result.Behind =>
            hop(start = mid + 1, end = end, knownLowest = Some(behind.previous), knownMatch = knownMatch)

          case aheadNoneOrEnd: KeyMatcher.Result.AheadOrNoneOrEnd =>
            aheadNoneOrEnd.ahead match {
              case Some(range: Persistent.Range) if ordering.gt(context.targetKey, range.fromKey) =>
                BinarySearchLowerResult.Some(
                  lower = None,
                  matched = Some(range)
                )

              case _ =>
                hop(start = start, end = mid - 1, knownLowest = knownLowest, knownMatch = knownMatch)
            }

        }
    }

    //println(s"lowestKey: ${context.lowestKeyValue.map(_.key.readInt())}, highestKey: ${context.highestKeyValue.map(_.key.readInt())}")

    val end = getEndPosition(context)

    if (fetchLeft) {
      hop(start = end - 1, end = end - 1, knownLowest = context.lowestKeyValue, knownMatch = None)
    } else {
      val start = getStartPosition(context)
      hop(start = start, end = end, knownLowest = context.lowestKeyValue, knownMatch = None)
    }
  }

  def search(key: Slice[Byte],
             lowest: Option[Persistent],
             highest: Option[Persistent],
             keyValuesCount: => Int,
             binarySearchIndexReader: Option[UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock]],
             sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
             valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]])(implicit ordering: KeyOrder[Slice[Byte]],
                                                                                     partialKeyOrder: KeyOrder[Persistent]): BinarySearchGetResult[Persistent] =
    if (sortedIndexReader.block.isNormalisedBinarySearchable) {
      //      binarySeeks += 1
      binarySearch(
        BinarySearchContext(
          key = key,
          lowest = lowest,
          highest = highest,
          keyValuesCount = keyValuesCount,
          sortedIndex = sortedIndexReader,
          values = valuesReader
        )
      )
    }
    else
      binarySearchIndexReader match {
        case Some(binarySearchIndexReader) =>
          //println
          //println(s"Key: ${key.readInt()}")
          //          hops = 0
          //          binarySeeks += 1
          //          maxHop = maxHop max currentHops
          //          minHop = minHop min currentHops
          //          currentHops = 0

          binarySearch(
            BinarySearchContext(
              key = key,
              lowest = lowest,
              highest = highest,
              binarySearchIndex = binarySearchIndexReader,
              sortedIndex = sortedIndexReader,
              values = valuesReader
            )
          ) match {
            case some: BinarySearchGetResult.Some[Persistent] =>
              //              binarySuccessfulSeeks += 1
              some

            case none: BinarySearchGetResult.None[Persistent] =>
              //              binaryFailedSeeks += 1
              if (binarySearchIndexReader.block.isFullIndex && !sortedIndexReader.block.hasPrefixCompression)
                none
              else
                none.lower match {
                  case Some(lower) =>
                    //                    failedWithLower += 1
                    //                    if (lowest.exists(lowest => ordering.gt(lower.key, lowest.key)))
                    //                      greaterLower += 1
                    //                    else if (lowest.exists(lowest => ordering.equiv(lower.key, lowest.key)))
                    //                      sameLower += 1

                    SortedIndexBlock.matchOrSeek(
                      key = key,
                      startFrom = lower,
                      sortedIndexReader = sortedIndexReader,
                      valuesReader = valuesReader
                    ) match {
                      case Some(got) =>
                        new BinarySearchGetResult.Some(got)

                      case None =>
                        new BinarySearchGetResult.None(Some(lower))
                    }

                  case None =>
                    SortedIndexBlock.seekAndMatch(
                      key = key,
                      startFrom = None,
                      sortedIndexReader = sortedIndexReader,
                      valuesReader = valuesReader
                    ) match {
                      case Some(got) =>
                        new BinarySearchGetResult.Some(got)

                      case None =>
                        BinarySearchGetResult.none
                    }
                }
          }

        case None =>
          SortedIndexBlock.seekAndMatch(
            key = key,
            startFrom = None,
            sortedIndexReader = sortedIndexReader,
            valuesReader = valuesReader
          ) match {
            case Some(got) =>
              new BinarySearchGetResult.Some(got)

            case None =>
              BinarySearchGetResult.none
          }
      }

  //it's assumed that input param start will not be a higher value of key.
  def searchHigher(key: Slice[Byte],
                   start: Option[Persistent],
                   end: Option[Persistent],
                   keyValuesCount: => Int,
                   binarySearchIndexReader: Option[UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock]],
                   sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                   valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]])(implicit ordering: KeyOrder[Slice[Byte]],
                                                                                           partialKeyOrder: KeyOrder[Persistent]): Option[Persistent] =
    when(start.exists(start => ordering.equiv(start.key, key)))(start) match {
      case Some(start) =>
        Some(
          SortedIndexBlock.readNextKeyValue(
            previous = start,
            sortedIndexReader = sortedIndexReader,
            valuesReader = valuesReader
          )
        )

      case None =>
        search(
          key = key,
          lowest = start,
          highest = end,
          keyValuesCount = keyValuesCount,
          binarySearchIndexReader = binarySearchIndexReader,
          sortedIndexReader = sortedIndexReader,
          valuesReader = valuesReader
        ) match {
          case none: BinarySearchGetResult.None[Persistent] =>
            SortedIndexBlock.matchOrSeekHigher(
              key = key,
              startFrom = none.lower,
              sortedIndexReader = sortedIndexReader,
              valuesReader = valuesReader
            )

          case some: BinarySearchGetResult.Some[Persistent] =>
            SortedIndexBlock.matchOrSeekHigher(
              key = key,
              startFrom = Some(some.value),
              sortedIndexReader = sortedIndexReader,
              valuesReader = valuesReader
            )
        }
    }

  private def resolveLowerFromBinarySearch(key: Slice[Byte],
                                           lower: Option[Persistent],
                                           got: Option[Persistent],
                                           end: Option[Persistent],
                                           sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                                           valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]])(implicit ordering: KeyOrder[Slice[Byte]]) = {
    //next can either be got or end if end is inline with lower.
    val next =
      if (end.exists(end => lower.exists(_.nextIndexOffset == end.indexOffset)))
        end
      else if (got.exists(got => lower.exists(_.nextIndexOffset == got.indexOffset)))
        got
      else
        None

    SortedIndexBlock.matchOrSeekLower(
      key = key,
      startFrom = lower,
      next = next,
      sortedIndexReader = sortedIndexReader,
      valuesReader = valuesReader
    )
  }

  def searchLower(key: Slice[Byte],
                  start: Option[Persistent],
                  end: Option[Persistent],
                  keyValuesCount: => Int,
                  binarySearchIndexReader: Option[UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock]],
                  sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                  valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]])(implicit ordering: KeyOrder[Slice[Byte]],
                                                                                          partialOrdering: KeyOrder[Persistent]): Option[Persistent] =
    if (sortedIndexReader.block.isNormalisedBinarySearchable) {
      val result =
        binarySearchLower(
          fetchLeft =
            //cannot shiftLeft is it's accessPosition is not known at start.
            //but there will be cases with binarySearchIndex is partial || sortedIndex is prefixCompressed
            //which means that accessPositions might not be in sync with binarySearch's positions.
            //Here binarySearchLower will triggers are restart if shiftLeft was not successful.
            sortedIndexReader.block.enableAccessPositionIndex && end.exists(end => ordering.equiv(key, end.key)),
          context =
            BinarySearchContext(
              key = key,
              lowest = start,
              highest = end,
              keyValuesCount = keyValuesCount,
              sortedIndex = sortedIndexReader,
              values = valuesReader
            )
        )

      if (result.lower.isEmpty && result.matched.isEmpty)
        None
      else
        resolveLowerFromBinarySearch(
          key = key,
          lower = result.lower,
          got = result.matched,
          end = end,
          sortedIndexReader = sortedIndexReader,
          valuesReader = valuesReader
        )
    }
    else
      binarySearchIndexReader match {
        case Some(binarySearchIndexReader) =>
          val result =
            binarySearchLower(
              fetchLeft =
                sortedIndexReader.block.enableAccessPositionIndex && end.exists(end => ordering.equiv(key, end.key)),
              context =
                BinarySearchContext(
                  key = key,
                  lowest = start,
                  highest = end,
                  binarySearchIndex = binarySearchIndexReader,
                  sortedIndex = sortedIndexReader,
                  values = valuesReader
                )
            )

          resolveLowerFromBinarySearch(
            key = key,
            lower = result.lower,
            got = result.matched,
            end = end,
            sortedIndexReader = sortedIndexReader,
            valuesReader = valuesReader
          )

        case None =>
          SortedIndexBlock.seekLowerAndMatch(
            key = key,
            startFrom = start,
            sortedIndexReader = sortedIndexReader,
            valuesReader = valuesReader
          )
      }

  implicit object BinarySearchIndexBlockOps extends BlockOps[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock] {
    override def updateBlockOffset(block: BinarySearchIndexBlock, start: Int, size: Int): BinarySearchIndexBlock =
      block.copy(offset = BinarySearchIndexBlock.Offset(start = start, size = size))

    override def createOffset(start: Int, size: Int): Offset =
      BinarySearchIndexBlock.Offset(start, size)

    override def readBlock(header: Block.Header[Offset]): BinarySearchIndexBlock =
      BinarySearchIndexBlock.read(header)
  }

}

private[core] case class BinarySearchIndexBlock(offset: BinarySearchIndexBlock.Offset,
                                                valuesCount: Int,
                                                headerSize: Int,
                                                bytesPerValue: Int,
                                                isFullIndex: Boolean,
                                                compressionInfo: Option[Block.CompressionInfo]) extends Block[BinarySearchIndexBlock.Offset] {
  val isUnsignedInt: Boolean =
    BinarySearchIndexBlock.isUnsignedInt(bytesPerValue)
}
