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

import swaydb.IO
import swaydb.core.data.{Persistent, PersistentOption}
import swaydb.core.segment.block._
import swaydb.core.segment.block.reader.UnblockedReader
import swaydb.core.segment.block.sortedindex.SortedIndexBlock
import swaydb.core.segment.block.values.ValuesBlock
import swaydb.core.util.MinMax
import swaydb.data.config.UncompressedBlockInfo
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.utils.Maybe.{Maybe, _}

import scala.annotation.tailrec

private[core] case object BinarySearchIndexBlock {

  val blockName = this.productPrefix

  case class Offset(start: Int, size: Int) extends BlockOffset

  def init(sortedIndexState: SortedIndexBlock.State,
           binarySearchConfig: BinarySearchIndexConfig): Option[BinarySearchIndexState] = {

    if (!binarySearchConfig.enabled ||
      sortedIndexState.uncompressedPrefixCount < binarySearchConfig.minimumNumberOfKeys ||
      sortedIndexState.normaliseIndex ||
      (!sortedIndexState.hasPrefixCompression && binarySearchConfig.searchSortedIndexDirectlyIfPossible && sortedIndexState.isPreNormalised))
      None
    else
      BinarySearchIndexState(
        format = binarySearchConfig.format,
        largestIndexOffset = sortedIndexState.secondaryIndexEntries.last.indexOffset,
        largestMergedKeySize = sortedIndexState.largestUncompressedMergedKeySize,
        //not using size from stats because it's size does not account for hashIndex's missed keys.
        uniqueValuesCount = sortedIndexState.uncompressedPrefixCount,
        isFullIndex = binarySearchConfig.fullIndex,
        minimumNumberOfKeys = binarySearchConfig.minimumNumberOfKeys,
        compressions = binarySearchConfig.compressions
      )
  }

  def optimalBytesRequired(largestIndexOffset: Int,
                           largestMergedKeySize: Int,
                           valuesCount: Int,
                           minimNumberOfKeysForBinarySearchIndex: Int,
                           bytesToAllocatedPerEntryMaybe: Maybe[Int],
                           format: BinarySearchEntryFormat): Int =
    if (valuesCount < minimNumberOfKeysForBinarySearchIndex) {
      0
    } else {
      val bytesToAllocatedPerEntry = bytesToAllocatedPerEntryMaybe getOrElse {
        format.bytesToAllocatePerEntry(
          largestIndexOffset = largestIndexOffset,
          largestMergedKeySize = largestMergedKeySize
        )
      }

      bytesToAllocatedPerEntry * valuesCount
    }

  def close(state: BinarySearchIndexState, uncompressedKeyValuesCount: Int): Option[BinarySearchIndexState] =
    if (state.compressibleBytes.isEmpty)
      None
    else if (state.hasMinimumKeys) {
      val compressionResult =
        Block.compress(
          bytes = state.compressibleBytes,
          compressions = state.compressions(UncompressedBlockInfo(state.compressibleBytes.size)),
          blockName = blockName
        )

      compressionResult.compressedBytes foreach (state.compressibleBytes = _)

      compressionResult.headerBytes add state.format.id
      compressionResult.headerBytes addUnsignedInt state.writtenValues
      compressionResult.headerBytes addInt state.bytesPerValue
      state.isFullIndex = state.writtenValues == uncompressedKeyValuesCount
      compressionResult.headerBytes addBoolean state.isFullIndex

      compressionResult.fixHeaderSize()

      state.header = compressionResult.headerBytes

      //      if (state.bytes.currentWritePosition > state.headerSize)
      //        throw IO.throwable(s"Calculated header size was incorrect. Expected: ${state.headerSize}. Used: ${state.bytes.currentWritePosition - 1}")
      Some(state)
    }
    else
      None

  def unblockedReader(closedState: BinarySearchIndexState): UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock] = {
    val block =
      BinarySearchIndexBlock(
        format = closedState.format,
        offset = BinarySearchIndexBlock.Offset(0, closedState.cacheableBytes.size),
        valuesCount = closedState.writtenValues,
        headerSize = 0,
        bytesPerValue = closedState.bytesPerValue,
        isFullIndex = closedState.isFullIndex,
        compressionInfo = None
      )

    UnblockedReader(
      block = block,
      bytes = closedState.cacheableBytes.close()
    )
  }

  def read(header: BlockHeader[BinarySearchIndexBlock.Offset]): BinarySearchIndexBlock = {
    val formatId = header.headerReader.get()
    val format: BinarySearchEntryFormat = BinarySearchEntryFormat.formats.find(_.id == formatId) getOrElse IO.throws(s"Invalid binary search formatId: $formatId")
    val valuesCount = header.headerReader.readUnsignedInt()
    val bytesPerValue = header.headerReader.readInt()
    val isFullIndex = header.headerReader.readBoolean()

    BinarySearchIndexBlock(
      format = format,
      offset = header.offset,
      valuesCount = valuesCount,
      headerSize = header.headerSize,
      bytesPerValue = bytesPerValue,
      isFullIndex = isFullIndex,
      compressionInfo = header.compressionInfo
    )
  }

  def write(entry: SortedIndexBlock.SecondaryIndexEntry,
            state: BinarySearchIndexState): Unit =
    write(
      indexOffset = entry.indexOffset,
      mergedKey = entry.mergedKey,
      keyType = entry.keyType,
      state = state
    )

  def write(indexOffset: Int,
            mergedKey: Slice[Byte],
            keyType: Byte,
            state: BinarySearchIndexState): Unit =
    if (indexOffset == state.previouslyWritten) { //do not write duplicate entries.
      ()
    } else {
      //      if (state.bytes.size == 0) state.bytes moveWritePosition state.headerSize //if this the first write then skip the header bytes.
      val writePosition = state.compressibleBytes.currentWritePosition

      state.format.write(
        indexOffset = indexOffset,
        mergedKey = mergedKey,
        keyType = keyType,
        bytes = state.compressibleBytes
      )

      val missedBytes = state.bytesPerValue - (state.compressibleBytes.currentWritePosition - writePosition)
      if (missedBytes > 0)
        state.compressibleBytes moveWritePosition (state.compressibleBytes.currentWritePosition + missedBytes) //fill in the missing bytes to maintain fixed size for each entry.

      state.incrementWrittenValuesCount()
      state.previouslyWritten = indexOffset
    }

  //sortedIndexAccessPositions start from 1 but BinarySearch starts from 0.
  //A 0 sortedIndexAccessPosition indicates that sortedIndexAccessPositionIndex was disabled.
  //A key-values sortedIndexAccessPosition can sometimes be larger than what binarySearchIndex knows for cases where binarySearchIndex is partial
  //to handle that check that sortedIndexAccessPosition is not over the number total binarySearchIndex entries.
  def getSortedIndexAccessPosition(keyValue: Persistent, isFullIndex: Boolean, valuesCount: Int, default: Int): Int =
    if (keyValue.sortedIndexAccessPosition <= 0 || (!isFullIndex && keyValue.sortedIndexAccessPosition > valuesCount))
      default
    else
      keyValue.sortedIndexAccessPosition - 1

  def getStartPosition(lowestKeyValue: PersistentOption, isFullIndex: Boolean, valuesCount: Int): Int =
    lowestKeyValue match {
      case lowestKeyValue: Persistent =>
        getSortedIndexAccessPosition(lowestKeyValue, isFullIndex, valuesCount, 0)

      case Persistent.Null =>
        0
    }

  def getEndPosition(highestKeyValue: PersistentOption, isFullIndex: Boolean, valuesCount: Int): Int =
    highestKeyValue match {
      case highestKeyValue: Persistent =>
        getSortedIndexAccessPosition(highestKeyValue, isFullIndex, valuesCount, valuesCount - 1)

      case Persistent.Null =>
        valuesCount - 1
    }

  //  var totalHops = 0
  //  var maxHop = 0
  //  var minHop = 0
  //  var currentHops = 0
  //  var binarySeeks = 0
  //  var binarySuccessfulDirectSeeks = 0
  //  var binarySuccessfulSeeksWithWalkForward = 0
  //  var binaryFailedSeeks = 0
  //  var failedWithLower = 0
  //  var sameLower = 0
  //  var greaterLower = 0

  private[block] def binarySearchMatchOrLower(key: Slice[Byte],
                                              lowest: PersistentOption,
                                              highest: PersistentOption,
                                              keyValuesCount: => Int,
                                              binarySearchIndex: UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock],
                                              sortedIndex: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                                              valuesOrNull: UnblockedReader[ValuesBlock.Offset, ValuesBlock])(implicit order: KeyOrder[Persistent.Partial],
                                                                                                              keyOrder: KeyOrder[Slice[Byte]]): Persistent.PartialOption = {
    val isFullIndex = binarySearchIndex == null || binarySearchIndex.block.isFullIndex
    val valuesCount = if (binarySearchIndex == null) keyValuesCount else binarySearchIndex.block.valuesCount
    val bytesPerValue = if (binarySearchIndex == null) sortedIndex.block.segmentMaxIndexEntrySize else binarySearchIndex.block.bytesPerValue

    var start =
      getStartPosition(
        lowestKeyValue = lowest,
        isFullIndex = isFullIndex,
        valuesCount = valuesCount
      )

    var end =
      getEndPosition(
        highestKeyValue = highest,
        isFullIndex = isFullIndex,
        valuesCount = valuesCount
      )

    var knownLowest: Persistent.PartialOption = Persistent.Partial.Null

    while (start <= end) {
      //      totalHops += 1
      //            currentHops += 1

      val mid = start + (end - start) / 2

      val offset = mid * bytesPerValue

      //      println(s"start: $start, mid: $mid, end: $end")
      val partial =
        if (binarySearchIndex == null)
          SortedIndexBlock.readPartialKeyValue(
            fromOffset = offset,
            sortedIndexReader = sortedIndex,
            valuesReaderOrNull = valuesOrNull
          ).matchMutateForBinarySearch(key)
        else
          binarySearchIndex.block.format.read(
            offset = offset,
            seekSize = binarySearchIndex.block.bytesPerValue,
            binarySearchIndex = binarySearchIndex,
            sortedIndex = sortedIndex,
            valuesOrNull = valuesOrNull
          ).matchMutateForBinarySearch(key)

      if (partial.isBinarySearchMatched) {
        return partial
      } else if (partial.isBinarySearchBehind) {
        start = mid + 1
        knownLowest = partial
      } else if (partial.isBinarySearchAhead) {
        end = mid - 1
      } else {
        throw new Exception("Invalid binarySearch mutated flags")
      }
    }

    MinMax.maxFavourLeftC[Persistent.PartialOption, Persistent.Partial](
      left = knownLowest,
      right = lowest.asPartial
    )
  }

  private def binarySearchLower(fetchLeft: Boolean,
                                key: Slice[Byte],
                                lowest: PersistentOption,
                                highest: PersistentOption,
                                keyValuesCount: => Int,
                                binarySearchIndex: UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock],
                                sortedIndex: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                                valuesOrNull: UnblockedReader[ValuesBlock.Offset, ValuesBlock])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                                                partialOrder: KeyOrder[Persistent.Partial]): BinarySearchLowerResult.Some = {

    val isFullIndex = binarySearchIndex == null || binarySearchIndex.block.isFullIndex
    val valuesCount = if (binarySearchIndex == null) keyValuesCount else binarySearchIndex.block.valuesCount
    val bytesPerValue = if (binarySearchIndex == null) sortedIndex.block.segmentMaxIndexEntrySize else binarySearchIndex.block.bytesPerValue

    @tailrec
    def hop(start: Int, end: Int, knownLowest: Persistent.PartialOption, knownMatch: Persistent.PartialOption): BinarySearchLowerResult.Some = {
      val mid = start + (end - start) / 2

      //      println(s"start: $start, mid: $mid, end: $end, fetchLeft: $fetchLeft")

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
      if (start > end || mid < 0) {
        if (fetchLeft && knownLowest.isNoneC) {
          //println("Restart")
          binarySearchLower(
            fetchLeft = false,
            key = key,
            lowest = lowest,
            highest = highest,
            keyValuesCount = keyValuesCount,
            binarySearchIndex = binarySearchIndex,
            sortedIndex = sortedIndex,
            valuesOrNull = valuesOrNull
          )
        } else {
          //println("End")
          val lower =
            MinMax.maxFavourLeftC[Persistent.PartialOption, Persistent.Partial](
              left = knownLowest,
              right = lowest getOrElseS Persistent.Partial.Null
            )

          new BinarySearchLowerResult.Some(
            lower = lower,
            matched = knownMatch
          )
        }
      } else {
        val offset = mid * bytesPerValue

        val partial =
          if (binarySearchIndex == null)
            SortedIndexBlock.readPartialKeyValue(
              fromOffset = offset,
              sortedIndexReader = sortedIndex,
              valuesReaderOrNull = valuesOrNull
            ).matchMutateForBinarySearch(key)
          else
            binarySearchIndex.block.format.read(
              offset = offset,
              seekSize = binarySearchIndex.block.bytesPerValue,
              binarySearchIndex = binarySearchIndex,
              sortedIndex = sortedIndex,
              valuesOrNull = valuesOrNull
            ).matchMutateForBinarySearch(key)

        if (partial.isBinarySearchMatched)
          partial match {
            case fixed: Persistent.Partial.Fixed =>
              hop(start = mid - 1, end = mid - 1, knownLowest = knownLowest, knownMatch = fixed)

            case range: Persistent.Partial.Range =>
              if (keyOrder.gt(key, range.fromKey))
                new BinarySearchLowerResult.Some(
                  lower = Persistent.Partial.Null,
                  matched = range
                )
              else
                hop(start = mid - 1, end = mid - 1, knownLowest = knownLowest, knownMatch = range)
          }
        else if (partial.isBinarySearchBehind)
          hop(start = mid + 1, end = end, knownLowest = partial, knownMatch = knownMatch)
        else if (partial.isBinarySearchAhead)
          partial match {
            case range: Persistent.Partial.Range if keyOrder.gt(key, range.fromKey) =>
              new BinarySearchLowerResult.Some(
                lower = Persistent.Partial.Null,
                matched = range
              )

            case _ =>
              hop(start = start, end = mid - 1, knownLowest = knownLowest, knownMatch = knownMatch)
          }
        else
          throw new Exception("Invalid binary search mutated flags")
      }
    }

    //println(s"lowestKey: ${context.lowestKeyValue.map(_.key.readInt())}, highestKey: ${context.highestKeyValue.map(_.key.readInt())}")

    val end =
      getEndPosition(
        highestKeyValue = highest,
        isFullIndex = isFullIndex,
        valuesCount = valuesCount
      )

    if (fetchLeft) {
      hop(start = end - 1, end = end - 1, knownLowest = lowest.asPartial, knownMatch = Persistent.Partial.Null)
    } else {
      val start =
        getStartPosition(
          lowestKeyValue = lowest,
          isFullIndex = isFullIndex,
          valuesCount = valuesCount
        )

      hop(start = start, end = end, knownLowest = lowest.asPartial, knownMatch = Persistent.Partial.Null)
    }
  }

  def search(key: Slice[Byte],
             lowest: PersistentOption,
             highest: PersistentOption,
             keyValuesCount: => Int,
             binarySearchIndexReaderOrNull: UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock],
             sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
             valuesReaderOrNull: UnblockedReader[ValuesBlock.Offset, ValuesBlock])(implicit ordering: KeyOrder[Slice[Byte]],
                                                                                   partialKeyOrder: KeyOrder[Persistent.Partial]): Persistent.PartialOption =
    if (sortedIndexReader.block.isBinarySearchable) {
      //      binarySeeks += 1
      binarySearchMatchOrLower(
        key = key,
        lowest = lowest,
        highest = highest,
        keyValuesCount = keyValuesCount,
        binarySearchIndex = null,
        sortedIndex = sortedIndexReader,
        valuesOrNull = valuesReaderOrNull
      ) match {
        case partial: Persistent.Partial if partial.isBinarySearchMatched =>
          partial

        case _ =>
          Persistent.Partial.Null
      }
    } else if (binarySearchIndexReaderOrNull == null) {
      SortedIndexBlock.seekAndMatch(
        key = key,
        startFrom = Persistent.Null,
        sortedIndexReader = sortedIndexReader,
        valuesReaderOrNull = valuesReaderOrNull
      ).asPartial
    } else {
      //println(s"Key: ${key.readInt()}")
      //          hops = 0
      //      binarySeeks += 1
      //          maxHop = maxHop max currentHops
      //          minHop = minHop min currentHops
      //          currentHops = 0

      binarySearchMatchOrLower(
        key = key,
        lowest = lowest,
        highest = highest,
        keyValuesCount = keyValuesCount,
        binarySearchIndex = binarySearchIndexReaderOrNull,
        sortedIndex = sortedIndexReader,
        valuesOrNull = valuesReaderOrNull
      ) match {
        case partial: Persistent.Partial if partial.isBinarySearchMatched =>
          //          binarySuccessfulDirectSeeks += 1
          partial

        case lowerOrNone =>
          if (binarySearchIndexReaderOrNull.block.isFullIndex && !sortedIndexReader.block.hasPrefixCompression) {
            //            binaryFailedSeeks += 1
            Persistent.Partial.Null
          } else {
            val startFrom = lowerOrNone.toPersistentOptional
            if (startFrom.isNoneS || startFrom.existsS(_.hasMore))
              SortedIndexBlock.seekAndMatch(
                key = key,
                startFrom = startFrom,
                sortedIndexReader = sortedIndexReader,
                valuesReaderOrNull = valuesReaderOrNull
              ).asPartial
            else
              Persistent.Partial.Null
          }
      }
    }

  //it's assumed that input param start will not be a higher value of key.
  def searchHigher(key: Slice[Byte],
                   start: PersistentOption,
                   end: PersistentOption,
                   keyValuesCount: => Int,
                   binarySearchIndexReaderOrNull: UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock],
                   sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                   valuesReaderOrNull: UnblockedReader[ValuesBlock.Offset, ValuesBlock])(implicit ordering: KeyOrder[Slice[Byte]],
                                                                                         partialKeyOrder: KeyOrder[Persistent.Partial]): PersistentOption = {
    val startFrom =
      search( //A check to see if key equiv start.key to perform a simple forward seek without matching is done in SegmentSearcher
        key = key,
        lowest = start,
        highest = end,
        keyValuesCount = keyValuesCount,
        binarySearchIndexReaderOrNull = binarySearchIndexReaderOrNull,
        sortedIndexReader = sortedIndexReader,
        valuesReaderOrNull = valuesReaderOrNull
      )

    SortedIndexBlock.matchOrSeekHigher(
      key = key,
      startFrom = startFrom.toPersistentOptional,
      sortedIndexReader = sortedIndexReader,
      valuesReaderOrNull = valuesReaderOrNull
    )
  }

  private def resolveLowerFromBinarySearch(key: Slice[Byte],
                                           lower: PersistentOption,
                                           got: PersistentOption,
                                           end: PersistentOption,
                                           sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                                           valuesReaderOrNull: UnblockedReader[ValuesBlock.Offset, ValuesBlock])(implicit ordering: KeyOrder[Slice[Byte]]): PersistentOption = {
    val next =
      if (end.existsS(end => lower.existsS(_.nextIndexOffset == end.indexOffset)))
        end
      else if (got.existsS(got => lower.existsS(_.nextIndexOffset == got.indexOffset)))
        got
      else
        Persistent.Null

    SortedIndexBlock.matchOrSeekLower(
      key = key,
      startFrom = lower,
      next = next,
      sortedIndexReader = sortedIndexReader,
      valuesReaderOrNull = valuesReaderOrNull
    )
  }

  def searchLower(key: Slice[Byte],
                  start: PersistentOption,
                  end: PersistentOption,
                  keyValuesCount: Int,
                  binarySearchIndexReaderOrNull: UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock],
                  sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                  valuesReaderOrNull: UnblockedReader[ValuesBlock.Offset, ValuesBlock])(implicit ordering: KeyOrder[Slice[Byte]],
                                                                                        partialOrdering: KeyOrder[Persistent.Partial]): PersistentOption =
    if (sortedIndexReader.block.isBinarySearchable) {
      val result =
        binarySearchLower(
          //cannot shiftLeft is it's accessPosition is not known at start.
          //but there will be cases with binarySearchIndex is partial || sortedIndex is prefixCompressed
          //which means that accessPositions might not be in sync with binarySearch's positions.
          //Here binarySearchLower will triggers are restart if shiftLeft was not successful.
          fetchLeft = !sortedIndexReader.block.optimiseForReverseIteration && sortedIndexReader.block.enableAccessPositionIndex && end.existsS(end => ordering.equiv(key, end.key)),
          key = key,
          lowest = start,
          highest = end,
          keyValuesCount = keyValuesCount,
          binarySearchIndex = null,
          sortedIndex = sortedIndexReader,
          valuesOrNull = valuesReaderOrNull
        )

      if (result.lower.isNoneC && result.matched.isNoneC)
        Persistent.Null
      else
        resolveLowerFromBinarySearch(
          key = key,
          lower = result.lower.toPersistentOptional,
          got = result.matched.toPersistentOptional,
          end = end,
          sortedIndexReader = sortedIndexReader,
          valuesReaderOrNull = valuesReaderOrNull
        )
    } else if (binarySearchIndexReaderOrNull == null) {
      SortedIndexBlock.seekLowerAndMatch(
        key = key,
        startFrom = start,
        sortedIndexReader = sortedIndexReader,
        valuesReaderOrNull = valuesReaderOrNull
      )
    } else {
      val result =
        binarySearchLower(
          fetchLeft = !sortedIndexReader.block.optimiseForReverseIteration && sortedIndexReader.block.enableAccessPositionIndex && end.existsS(end => ordering.equiv(key, end.key)),
          key = key,
          lowest = start,
          highest = end,
          keyValuesCount = keyValuesCount,
          binarySearchIndex = binarySearchIndexReaderOrNull,
          sortedIndex = sortedIndexReader,
          valuesOrNull = valuesReaderOrNull
        )

      resolveLowerFromBinarySearch(
        key = key,
        lower = result.lower.toPersistentOptional,
        got = result.matched.toPersistentOptional,
        end = end,
        sortedIndexReader = sortedIndexReader,
        valuesReaderOrNull = valuesReaderOrNull
      )
    }

  implicit object BinarySearchIndexBlockOps extends BlockOps[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock] {
    override def updateBlockOffset(block: BinarySearchIndexBlock, start: Int, size: Int): BinarySearchIndexBlock =
      block.copy(offset = BinarySearchIndexBlock.Offset(start = start, size = size))

    override def createOffset(start: Int, size: Int): Offset =
      BinarySearchIndexBlock.Offset(start, size)

    override def readBlock(header: BlockHeader[Offset]): BinarySearchIndexBlock =
      BinarySearchIndexBlock.read(header)
  }

}

private[core] case class BinarySearchIndexBlock(format: BinarySearchEntryFormat,
                                                offset: BinarySearchIndexBlock.Offset,
                                                valuesCount: Int,
                                                headerSize: Int,
                                                bytesPerValue: Int,
                                                isFullIndex: Boolean,
                                                compressionInfo: Option[BlockCompressionInfo]) extends Block[BinarySearchIndexBlock.Offset]
