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
import swaydb.core.data.{Persistent, PersistentOptional}
import swaydb.core.segment.format.a.block._
import swaydb.core.segment.format.a.block.reader.UnblockedReader
import swaydb.core.util.MinMax
import swaydb.data.config.{IOAction, IOStrategy, UncompressedBlockInfo}
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.data.util.Maybe.{Maybe, _}
import swaydb.data.util.{Functions, Maybe}

import scala.annotation.tailrec

private[core] object BinarySearchIndexBlock {

  val blockName = this.getClass.getSimpleName.dropRight(1)

  object Config {

    val disabled =
      Config(
        enabled = false,
        format = BinarySearchEntryFormat.Reference,
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
            format = BinarySearchEntryFormat.Reference,
            minimumNumberOfKeys = Int.MaxValue,
            fullIndex = false,
            searchSortedIndexDirectlyIfPossible = searchSortedIndexDirectly,
            ioStrategy = dataType => IOStrategy.SynchronisedIO(cacheOnAccess = dataType.isCompressed),
            compressions = _ => Seq.empty
          )

        case enable: swaydb.data.config.BinarySearchIndex.FullIndex =>
          Config(
            enabled = true,
            format = BinarySearchEntryFormat(enable.indexFormat),
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
            format = BinarySearchEntryFormat(enable.indexFormat),
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
                    format: BinarySearchEntryFormat,
                    minimumNumberOfKeys: Int,
                    searchSortedIndexDirectlyIfPossible: Boolean,
                    fullIndex: Boolean,
                    ioStrategy: IOAction => IOStrategy,
                    compressions: UncompressedBlockInfo => Seq[CompressionInternal])

  case class Offset(start: Int, size: Int) extends BlockOffset

  object State {
    def apply(format: BinarySearchEntryFormat,
              largestIndexOffset: Int,
              largestMergedKeySize: Int,
              uniqueValuesCount: Int,
              isFullIndex: Boolean,
              minimumNumberOfKeys: Int,
              compressions: UncompressedBlockInfo => Seq[CompressionInternal]): Option[State] =
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

        val bytes = Slice.create[Byte](bytesRequired)

        val state =
          new State(
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

  class State(val format: BinarySearchEntryFormat,
              val bytesPerValue: Int,
              val uniqueValuesCount: Int,
              var _previousWritten: Int,
              var writtenValues: Int,
              val minimumNumberOfKeys: Int,
              var isFullIndex: Boolean,
              var compressibleBytes: Slice[Byte],
              val cacheableBytes: Slice[Byte],
              var header: Slice[Byte],
              val compressions: UncompressedBlockInfo => Seq[CompressionInternal]) {

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

  def init(sortedIndexState: SortedIndexBlock.State,
           binarySearchConfig: BinarySearchIndexBlock.Config): Option[State] = {

    if (!binarySearchConfig.enabled ||
      sortedIndexState.uncompressedPrefixCount < binarySearchConfig.minimumNumberOfKeys ||
      sortedIndexState.normaliseIndex ||
      (!sortedIndexState.hasPrefixCompression && binarySearchConfig.searchSortedIndexDirectlyIfPossible && sortedIndexState.isPreNormalised))
      None
    else
      BinarySearchIndexBlock.State(
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

  def close(state: State, uncompressedKeyValuesCount: Int): Option[State] =
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

      compressionResult.headerBytes addByte state.format.id
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

  def unblockedReader(closedState: BinarySearchIndexBlock.State): UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock] = {
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

  def read(header: Block.Header[BinarySearchIndexBlock.Offset]): BinarySearchIndexBlock = {
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
            state: State): Unit =
    write(
      indexOffset = entry.indexOffset,
      mergedKey = entry.mergedKey,
      keyType = entry.keyType,
      state = state
    )

  def write(indexOffset: Int,
            mergedKey: Slice[Byte],
            keyType: Byte,
            state: State): Unit =
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

  def getStartPosition(lowestKeyValue: PersistentOptional, isFullIndex: Boolean, valuesCount: Int): Int =
    lowestKeyValue match {
      case lowestKeyValue: Persistent =>
        getSortedIndexAccessPosition(lowestKeyValue, isFullIndex, valuesCount, 0)

      case Persistent.Null =>
        0
    }

  def getEndPosition(highestKeyValue: PersistentOptional, isFullIndex: Boolean, valuesCount: Int): Int =
    highestKeyValue match {
      case highestKeyValue: Persistent =>
        getSortedIndexAccessPosition(highestKeyValue, isFullIndex, valuesCount, valuesCount - 1)

      case Persistent.Null =>
        valuesCount - 1
    }

  var totalHops = 0
  //  var maxHop = 0
  //  var minHop = 0
  //  var currentHops = 0
  var binarySeeks = 0
  var binarySuccessfulDirectSeeks = 0
  var binarySuccessfulSeeksWithWalkForward = 0
  var binaryFailedSeeks = 0
  //  var failedWithLower = 0
  //  var sameLower = 0
  //  var greaterLower = 0

  private[block] def binarySearch(context: BinarySearchContext)(implicit order: KeyOrder[Persistent.Partial]): Persistent.PartialOptional = {
    var start =
      getStartPosition(
        lowestKeyValue = context.lowestKeyValue,
        isFullIndex = context.isFullIndex,
        valuesCount = context.valuesCount
      )

    var end =
      getEndPosition(
        highestKeyValue = context.highestKeyValue,
        isFullIndex = context.isFullIndex,
        valuesCount = context.valuesCount
      )

    var knownLowest: Persistent.PartialOptional = Persistent.Partial.Null

    while (start <= end) {
      totalHops += 1
      //            currentHops += 1

      val mid = start + (end - start) / 2

      //      println(s"start: $start, mid: $mid, end: $end")
      val partial = context.seekAndMatchMutate(mid * context.bytesPerValue)

      if (partial.isBinarySearchMatched) {
        return partial
      } else if (partial.isBinarySearchBehind) {
        start = mid + 1
        knownLowest = partial
      } else if (partial.isBinarySearchAhead) {
        end = mid - 1
      } else {
        throw new Exception("Invalid binarySearch mutated state")
      }
    }

    MinMax.maxFavourLeftC[Persistent.PartialOptional, Persistent.Partial](
      left = knownLowest,
      right = context.lowestKeyValue.asPartial
    )
  }

  private def binarySearchLower(fetchLeft: Boolean, context: BinarySearchContext)(implicit ordering: KeyOrder[Slice[Byte]],
                                                                                  partialOrdering: KeyOrder[Persistent.Partial]): BinarySearchLowerResult.Some = {

    @tailrec
    def hop(start: Int, end: Int, knownLowest: Persistent.PartialOptional, knownMatch: Persistent.PartialOptional): BinarySearchLowerResult.Some = {
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
          binarySearchLower(fetchLeft = false, context = context)
        } else {
          //println("End")
          val lower =
            MinMax.maxFavourLeftC[Persistent.PartialOptional, Persistent.Partial](
              left = knownLowest,
              right = context.lowestKeyValue getOrElseS Persistent.Partial.Null
            )

          new BinarySearchLowerResult.Some(
            lower = lower,
            matched = knownMatch
          )
        }
      } else {
        val partial = context.seekAndMatchMutate(mid * context.bytesPerValue)
        if (partial.isBinarySearchMatched)
          partial match {
            case fixed: Persistent.Partial.Fixed =>
              hop(start = mid - 1, end = mid - 1, knownLowest = knownLowest, knownMatch = fixed)

            case range: Persistent.Partial.Range =>
              if (ordering.gt(context.targetKey, range.fromKey))
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
            case range: Persistent.Partial.Range if ordering.gt(context.targetKey, range.fromKey) =>
              new BinarySearchLowerResult.Some(
                lower = Persistent.Partial.Null,
                matched = range
              )

            case _ =>
              hop(start = start, end = mid - 1, knownLowest = knownLowest, knownMatch = knownMatch)
          }
        else
          throw new Exception("Invalid mutation")
      }
    }

    //println(s"lowestKey: ${context.lowestKeyValue.map(_.key.readInt())}, highestKey: ${context.highestKeyValue.map(_.key.readInt())}")

    val end =
      getEndPosition(
        highestKeyValue = context.highestKeyValue,
        isFullIndex = context.isFullIndex,
        valuesCount = context.valuesCount
      )

    if (fetchLeft) {
      hop(start = end - 1, end = end - 1, knownLowest = context.lowestKeyValue.asPartial, knownMatch = Persistent.Partial.Null)
    } else {
      val start =
        getStartPosition(
          lowestKeyValue = context.lowestKeyValue,
          isFullIndex = context.isFullIndex,
          valuesCount = context.valuesCount
        )

      hop(start = start, end = end, knownLowest = context.lowestKeyValue.asPartial, knownMatch = Persistent.Partial.Null)
    }
  }

  def search(key: Slice[Byte],
             lowest: PersistentOptional,
             highest: PersistentOptional,
             keyValuesCount: => Int,
             binarySearchIndexReaderNullable: UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock],
             sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
             valuesReaderNullable: UnblockedReader[ValuesBlock.Offset, ValuesBlock])(implicit ordering: KeyOrder[Slice[Byte]],
                                                                                     partialKeyOrder: KeyOrder[Persistent.Partial]): Persistent.PartialOptional =
    if (sortedIndexReader.block.isBinarySearchable) {
      binarySeeks += 1
      binarySearch(
        BinarySearchContext(
          key = key,
          lowest = lowest,
          highest = highest,
          keyValuesCount = keyValuesCount,
          sortedIndex = sortedIndexReader,
          valuesNullable = valuesReaderNullable
        )
      )
    } else if (binarySearchIndexReaderNullable == null) {
      SortedIndexBlock.seekAndMatch(
        key = key,
        startFrom = Persistent.Null,
        sortedIndexReader = sortedIndexReader,
        valuesReaderNullable = valuesReaderNullable
      ).asPartial
    } else {
      //println(s"Key: ${key.readInt()}")
      //          hops = 0
      binarySeeks += 1
      //          maxHop = maxHop max currentHops
      //          minHop = minHop min currentHops
      //          currentHops = 0

      binarySearch(
        BinarySearchContext(
          key = key,
          lowest = lowest,
          highest = highest,
          binarySearchIndex = binarySearchIndexReaderNullable,
          sortedIndex = sortedIndexReader,
          valuesNullable = valuesReaderNullable
        )
      ) match {
        case partial: Persistent.Partial if partial.isBinarySearchMatched =>
          binarySuccessfulDirectSeeks += 1
          partial

        case lowerOrNone =>
          if (binarySearchIndexReaderNullable.block.isFullIndex && !sortedIndexReader.block.hasPrefixCompression) {
            binaryFailedSeeks += 1
            Persistent.Partial.Null
          } else {
            val startFrom = lowerOrNone.toPersistentOptional
            if (startFrom.isNoneS || startFrom.existsS(_.hasMore))
              SortedIndexBlock.seekAndMatch(
                key = key,
                startFrom = startFrom,
                sortedIndexReader = sortedIndexReader,
                valuesReaderNullable = valuesReaderNullable
              ).asPartial
            else
              Persistent.Partial.Null
          }
      }
    }

  //it's assumed that input param start will not be a higher value of key.
  def searchHigher(key: Slice[Byte],
                   start: PersistentOptional,
                   end: PersistentOptional,
                   keyValuesCount: => Int,
                   binarySearchIndexReaderNullable: UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock],
                   sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                   valuesReaderNullable: UnblockedReader[ValuesBlock.Offset, ValuesBlock])(implicit ordering: KeyOrder[Slice[Byte]],
                                                                                           partialKeyOrder: KeyOrder[Persistent.Partial]): PersistentOptional = {
    val startFrom =
      search( //A check to see if key equiv start.key to perform a simple forward seek without matching is done in SegmentSearcher
        key = key,
        lowest = start,
        highest = end,
        keyValuesCount = keyValuesCount,
        binarySearchIndexReaderNullable = binarySearchIndexReaderNullable,
        sortedIndexReader = sortedIndexReader,
        valuesReaderNullable = valuesReaderNullable
      )

    SortedIndexBlock.matchOrSeekHigher(
      key = key,
      startFrom = startFrom.toPersistentOptional,
      sortedIndexReader = sortedIndexReader,
      valuesReaderNullable = valuesReaderNullable
    )
  }

  private def resolveLowerFromBinarySearch(key: Slice[Byte],
                                           lower: PersistentOptional,
                                           got: PersistentOptional,
                                           end: PersistentOptional,
                                           sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                                           valuesReaderNullable: UnblockedReader[ValuesBlock.Offset, ValuesBlock])(implicit ordering: KeyOrder[Slice[Byte]]): PersistentOptional = {
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
      valuesReaderNullable = valuesReaderNullable
    )
  }

  def searchLower(key: Slice[Byte],
                  start: PersistentOptional,
                  end: PersistentOptional,
                  keyValuesCount: Int,
                  binarySearchIndexReaderNullable: UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock],
                  sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                  valuesReaderNullable: UnblockedReader[ValuesBlock.Offset, ValuesBlock])(implicit ordering: KeyOrder[Slice[Byte]],
                                                                                          partialOrdering: KeyOrder[Persistent.Partial]): PersistentOptional =
    if (sortedIndexReader.block.isBinarySearchable) {
      val result =
        binarySearchLower(
          fetchLeft =
            //cannot shiftLeft is it's accessPosition is not known at start.
            //but there will be cases with binarySearchIndex is partial || sortedIndex is prefixCompressed
            //which means that accessPositions might not be in sync with binarySearch's positions.
            //Here binarySearchLower will triggers are restart if shiftLeft was not successful.
            sortedIndexReader.block.enableAccessPositionIndex && end.existsS(end => ordering.equiv(key, end.key)),
          context =
            BinarySearchContext(
              key = key,
              lowest = start,
              highest = end,
              keyValuesCount = keyValuesCount,
              sortedIndex = sortedIndexReader,
              valuesNullable = valuesReaderNullable
            )
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
          valuesReaderNullable = valuesReaderNullable
        )
    }
    else if (binarySearchIndexReaderNullable == null)
      SortedIndexBlock.seekLowerAndMatch(
        key = key,
        startFrom = start,
        sortedIndexReader = sortedIndexReader,
        valuesReaderNullable = valuesReaderNullable
      )
    else {
      val result =
        binarySearchLower(
          fetchLeft =
            sortedIndexReader.block.enableAccessPositionIndex && end.existsS(end => ordering.equiv(key, end.key)),
          context =
            BinarySearchContext(
              key = key,
              lowest = start,
              highest = end,
              binarySearchIndex = binarySearchIndexReaderNullable,
              sortedIndex = sortedIndexReader,
              valuesNullable = valuesReaderNullable
            )
        )

      resolveLowerFromBinarySearch(
        key = key,
        lower = result.lower.toPersistentOptional,
        got = result.matched.toPersistentOptional,
        end = end,
        sortedIndexReader = sortedIndexReader,
        valuesReaderNullable = valuesReaderNullable
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

private[core] case class BinarySearchIndexBlock(format: BinarySearchEntryFormat,
                                                offset: BinarySearchIndexBlock.Offset,
                                                valuesCount: Int,
                                                headerSize: Int,
                                                bytesPerValue: Int,
                                                isFullIndex: Boolean,
                                                compressionInfo: Option[Block.CompressionInfo]) extends Block[BinarySearchIndexBlock.Offset]
