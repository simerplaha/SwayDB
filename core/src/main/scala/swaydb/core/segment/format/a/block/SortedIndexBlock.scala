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

import com.typesafe.scalalogging.LazyLogging
import swaydb.Error.Segment.ExceptionHandler
import swaydb.IO
import swaydb.compression.CompressionInternal
import swaydb.core.data.{KeyValue, Persistent, Transient}
import swaydb.core.io.reader.Reader
import swaydb.core.segment.format.a.block.KeyMatcher.Result
import swaydb.core.segment.format.a.block.reader.UnblockedReader
import swaydb.core.segment.format.a.entry.reader.EntryReader
import swaydb.core.util.Bytes
import swaydb.data.config.{IOAction, IOStrategy, UncompressedBlockInfo}
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.data.util.{ByteSizeOf, Functions}

import scala.annotation.tailrec

private[core] object SortedIndexBlock extends LazyLogging {

  val blockName = this.getClass.getSimpleName.dropRight(1)

  implicit object SortedIndexBlockOps extends BlockOps[SortedIndexBlock.Offset, SortedIndexBlock] {
    override def updateBlockOffset(block: SortedIndexBlock, start: Int, size: Int): SortedIndexBlock =
      block.copy(offset = createOffset(start = start, size = size))

    override def createOffset(start: Int, size: Int): Offset =
      SortedIndexBlock.Offset(start = start, size = size)

    override def readBlock(header: Block.Header[Offset]): SortedIndexBlock =
      SortedIndexBlock.read(header)
  }

  object Config {
    val disabled =
      Config(
        ioStrategy = (dataType: IOAction) => IOStrategy.SynchronisedIO(cacheOnAccess = dataType.isCompressed),
        enableAccessPositionIndex = false,
        prefixCompressionResetCount = 0,
        disableKeyPrefixCompression = false,
        normaliseIndex = false,
        compressions = (_: UncompressedBlockInfo) => Seq.empty
      )

    def apply(config: swaydb.data.config.SortedKeyIndex): Config =
      config match {
        case config: swaydb.data.config.SortedKeyIndex.Enable =>
          apply(config)
      }

    def apply(enable: swaydb.data.config.SortedKeyIndex.Enable): Config =
      Config(
        enableAccessPositionIndex = enable.enablePositionIndex,
        prefixCompressionResetCount = enable.prefixCompression.resetCount max 0,
        disableKeyPrefixCompression = enable.prefixCompression.disableKeyPrefixCompression,
        normaliseIndex = enable.prefixCompression.normaliseIndexForBinarySearch,
        //cannot normalise if prefix compression is enabled.
        ioStrategy = Functions.safe(IOStrategy.synchronisedStoredIfCompressed, enable.ioStrategy),
        compressions =
          Functions.safe(
            default = (_: UncompressedBlockInfo) => Seq.empty[CompressionInternal],
            function = enable.compressions(_: UncompressedBlockInfo) map CompressionInternal.apply
          )
      )

    def apply(ioStrategy: IOAction => IOStrategy,
              prefixCompressionResetCount: Int,
              enableAccessPositionIndex: Boolean,
              normaliseIndex: Boolean,
              disableKeyPrefixCompression: Boolean,
              compressions: UncompressedBlockInfo => Seq[CompressionInternal]): Config =
      new Config(
        ioStrategy = ioStrategy,
        prefixCompressionResetCount = if (normaliseIndex) 0 else prefixCompressionResetCount max 0,
        enableAccessPositionIndex = enableAccessPositionIndex,
        //cannot normalise if prefix compression is enabled.
        normaliseIndex = normaliseIndex,
        disableKeyPrefixCompression = normaliseIndex || disableKeyPrefixCompression,
        compressions = compressions
      )
  }

  /**
   * Do not create [[Config]] directly. Use one of the apply functions.
   */
  class Config private(val ioStrategy: IOAction => IOStrategy,
                       val prefixCompressionResetCount: Int,
                       val enableAccessPositionIndex: Boolean,
                       val normaliseIndex: Boolean,
                       val disableKeyPrefixCompression: Boolean,
                       val compressions: UncompressedBlockInfo => Seq[CompressionInternal]) {

    def copy(ioStrategy: IOAction => IOStrategy = ioStrategy,
             prefixCompressionResetCount: Int = prefixCompressionResetCount,
             enableAccessPositionIndex: Boolean = enableAccessPositionIndex,
             normaliseIndex: Boolean = normaliseIndex,
             disableKeyPrefixCompression: Boolean = disableKeyPrefixCompression,
             compressions: UncompressedBlockInfo => Seq[CompressionInternal] = compressions) =
    //do not use new here. Submit this to the apply function to that rules for creating the config gets applied.
      Config(
        ioStrategy = ioStrategy,
        prefixCompressionResetCount = prefixCompressionResetCount,
        disableKeyPrefixCompression = disableKeyPrefixCompression,
        enableAccessPositionIndex = enableAccessPositionIndex,
        normaliseIndex = normaliseIndex,
        compressions = compressions
      )
  }

  case class Offset(start: Int, size: Int) extends BlockOffset

  case class State(var _bytes: Slice[Byte],
                   headerSize: Int,
                   hasPrefixCompression: Boolean,
                   enableAccessPositionIndex: Boolean,
                   normaliseIndex: Boolean,
                   isPreNormalised: Boolean, //indicates all entries already normalised without making any adjustments.
                   disableKeyPrefixCompression: Boolean,
                   segmentMaxIndexEntrySize: Int,
                   compressions: UncompressedBlockInfo => Seq[CompressionInternal]) {
    def bytes = _bytes

    def bytes_=(bytes: Slice[Byte]) =
      this._bytes = bytes
  }

  val hasCompressionHeaderSize = {
    val size =
      Block.headerSize(true) +
        ByteSizeOf.boolean + //enablePositionIndex
        ByteSizeOf.boolean + //normalisedForBinarySearch
        ByteSizeOf.boolean + //hasPrefixCompression
        ByteSizeOf.boolean + //isPreNormalised
        ByteSizeOf.boolean + //disableKeyPrefixCompression
        ByteSizeOf.varInt // segmentMaxSortedIndexEntrySize

    Bytes.sizeOfUnsignedInt(size) + size
  }

  val noCompressionHeaderSize = {
    val size =
      Block.headerSize(false) +
        ByteSizeOf.boolean + //enablePositionIndex
        ByteSizeOf.boolean + //normalisedForBinarySearch
        ByteSizeOf.boolean + //hasPrefixCompression
        ByteSizeOf.boolean + //isPreNormalised
        ByteSizeOf.boolean + //disableKeyPrefixCompression
        ByteSizeOf.varInt // segmentMaxSortedIndexEntrySize

    Bytes.sizeOfUnsignedInt(size) + size
  }

  def headerSize(hasCompression: Boolean): Int =
    if (hasCompression)
      hasCompressionHeaderSize
    else
      noCompressionHeaderSize

  def init(keyValues: Iterable[Transient]): (SortedIndexBlock.State, Iterable[Transient]) = {
    val isPreNormalised = keyValues.last.stats.hasSameIndexSizes()

    val normalisedKeyValues =
      if (!isPreNormalised && keyValues.last.sortedIndexConfig.normaliseIndex)
        Transient.normalise(keyValues)
      else
        keyValues

    val hasCompression =
      normalisedKeyValues
        .last
        .sortedIndexConfig
        .compressions(UncompressedBlockInfo(normalisedKeyValues.last.stats.segmentSortedIndexSize))
        .nonEmpty

    val headSize = headerSize(hasCompression)

    val bytes =
      if (hasCompression) //stats calculate size with no compression if there is compression add remaining bytes.
        Slice.create[Byte](normalisedKeyValues.last.stats.segmentSortedIndexSize - noCompressionHeaderSize + headSize)
      else
        Slice.create[Byte](normalisedKeyValues.last.stats.segmentSortedIndexSize)

    bytes moveWritePosition headSize

    val state =
      State(
        _bytes = bytes,
        headerSize = headSize,
        isPreNormalised = isPreNormalised,
        hasPrefixCompression = normalisedKeyValues.last.stats.hasPrefixCompression,
        enableAccessPositionIndex = normalisedKeyValues.last.sortedIndexConfig.enableAccessPositionIndex,
        disableKeyPrefixCompression = normalisedKeyValues.last.sortedIndexConfig.disableKeyPrefixCompression,
        normaliseIndex = normalisedKeyValues.last.sortedIndexConfig.normaliseIndex,
        segmentMaxIndexEntrySize = normalisedKeyValues.last.stats.segmentMaxSortedIndexEntrySize,
        compressions =
          //cannot have no compression to begin with a then have compression because that upsets the total bytes required.
          if (hasCompression)
            normalisedKeyValues.last.sortedIndexConfig.compressions
          else
            _ => Seq.empty
      )

    (state, normalisedKeyValues)
  }

  def write(keyValue: Transient, state: SortedIndexBlock.State): Unit =
    IO {
      state.bytes addAll keyValue.indexEntryBytes
    }

  def close(state: State): State = {
    val compressedOrUncompressedBytes =
      Block.block(
        headerSize = state.headerSize,
        bytes = state.bytes,
        compressions = state.compressions(UncompressedBlockInfo(state.bytes.size)),
        blockName = blockName
      )

    state.bytes = compressedOrUncompressedBytes
    state.bytes addBoolean state.enableAccessPositionIndex
    state.bytes addBoolean state.hasPrefixCompression
    state.bytes addBoolean state.normaliseIndex
    state.bytes addBoolean state.isPreNormalised
    state.bytes addBoolean state.disableKeyPrefixCompression
    state.bytes addUnsignedInt state.segmentMaxIndexEntrySize

    if (state.bytes.currentWritePosition > state.headerSize)
      throw IO.throwable(s"Calculated header size was incorrect. Expected: ${state.headerSize}. Used: ${state.bytes.currentWritePosition - 1}")
    else
      state
  }

  def read(header: Block.Header[SortedIndexBlock.Offset]): SortedIndexBlock = {
    val enableAccessPositionIndex = header.headerReader.readBoolean()
    val hasPrefixCompression = header.headerReader.readBoolean()
    val normaliseForBinarySearch = header.headerReader.readBoolean()
    val isPreNormalised = header.headerReader.readBoolean()
    val disableKeyPrefixCompression = header.headerReader.readBoolean()
    val segmentMaxIndexEntrySize = header.headerReader.readUnsignedInt()

    SortedIndexBlock(
      offset = header.offset,
      enableAccessPositionIndex = enableAccessPositionIndex,
      hasPrefixCompression = hasPrefixCompression,
      normaliseForBinarySearch = normaliseForBinarySearch,
      segmentMaxIndexEntrySize = segmentMaxIndexEntrySize,
      disableKeyPrefixCompression = disableKeyPrefixCompression,
      isPreNormalised = isPreNormalised,
      headerSize = header.headerSize,
      compressionInfo = header.compressionInfo
    )
  }

  private def readKeyValue(previous: Persistent,
                           indexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                           valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]]): Persistent =
    readKeyValue(
      indexEntrySizeMayBe = Some(previous.nextIndexSize),
      overwriteNextIndexOffset = None,
      indexReader = indexReader moveTo previous.nextIndexOffset,
      valuesReader = valuesReader,
      previous = Some(previous)
    )

  private def readKeyValue(fromPosition: Int,
                           overwriteNextIndexOffset: Option[Int],
                           indexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                           valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]]): Persistent =
    readKeyValue(
      indexEntrySizeMayBe = None,
      overwriteNextIndexOffset = overwriteNextIndexOffset,
      indexReader = indexReader moveTo fromPosition,
      valuesReader = valuesReader,
      previous = None
    )

  //  var ends = 0
  /**
   * Pre-requisite: The position of the index on the reader should be set.
   *
   * @param overwriteNextIndexOffset HashIndex with full copy store nextIndexOffset within themselves.
   *                                 This param overwrites calculation of nextIndexOffset from indexReader
   *                                 and applies this value if set. nextIndexOffset is used to
   *                                 calculate if the next key-value exists or if the the currently read
   *                                 key-value is in sequence with next.
   */
  private def readKeyValue(indexEntrySizeMayBe: Option[Int],
                           overwriteNextIndexOffset: Option[Int],
                           indexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                           valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
                           previous: Option[Persistent]): Persistent = {
    val positionBeforeRead = indexReader.getPosition

    //try reading entry bytes within one seek.
    val (indexSize, blockReader, isBlockReader) =
      indexEntrySizeMayBe match {
        case Some(indexEntrySize) if indexEntrySize > 0 =>
          indexReader skip Bytes.sizeOfUnsignedInt(indexEntrySize)
          (indexEntrySize, indexReader, false)

        case _ =>
          //if the reader has a block cache try fetching the bytes required within one seek.
          if (indexReader.hasBlockCache) {
            //read the minimum number of bytes required for parse this indexEntry.
            val bytes = indexReader.read(ByteSizeOf.varInt)
            val (indexEntrySize, indexEntrySizeByteSize) = bytes.readUnsignedIntWithByteSize()
            //open the slice if it's a subslice,
            val openBytes = bytes.openEnd()

            //check if the read bytes are enough to parse the entry.
            val expectedSize =
              if (overwriteNextIndexOffset.isDefined) //if overwrite is defined then next index's offset is not required.
                indexEntrySize + indexEntrySizeByteSize
              else //else next indexes's offset is required.
                indexEntrySize + indexEntrySizeByteSize + ByteSizeOf.varInt

            //if openBytes results in enough bytes to then read the open bytes only.
            if (openBytes.size >= expectedSize)
              (indexEntrySize, Reader(openBytes, indexEntrySizeByteSize), true)
            else {
              //              ends += 1
              (indexEntrySize, indexReader.moveTo(positionBeforeRead + indexEntrySizeByteSize), false)
            }
          } else {
            val indexEntrySize = indexReader.readUnsignedInt()
            (indexEntrySize, indexReader, false)
          }
      }

    //read all bytes for this index entry plus the next 5 bytes to fetch next index entry's size.
    val indexEntryBytesAndNextIndexEntrySize = blockReader read (indexSize + ByteSizeOf.varInt)

    val extraBytesRead = indexEntryBytesAndNextIndexEntrySize.size - indexSize

    //The above fetches another 5 bytes (unsigned int) along with previous index entry.
    //These 5 bytes contains the next index's size. Here the next key-values indexSize and indexOffset are read.

    val (nextIndexSize, nextIndexOffset) =
      if (extraBytesRead <= 0) {
        //no next key-value, next size is 0 and set offset to -1.
        (0, overwriteNextIndexOffset.getOrElse(-1))
      } else {
        //if extra tail byte were read this mean that this index has a next key-value.
        //next indexEntrySize is only read if it's required.

        val nextIndexEntrySize =
          indexEntryBytesAndNextIndexEntrySize
            .drop(indexSize)
            .readUnsignedInt()

        if (isBlockReader) {
          //openEnd() could read footer bytes so this check is needed.
          val nextIndexOffset = positionBeforeRead + blockReader.getPosition - extraBytesRead
          if (nextIndexOffset == indexReader.size)
            (0, -1)
          else
            (nextIndexEntrySize, nextIndexOffset)
        } else {
          (nextIndexEntrySize, blockReader.getPosition - extraBytesRead)
        }
      }

    //take only the bytes required for this in entry and submit it for parsing/reading.
    val indexEntry = indexEntryBytesAndNextIndexEntrySize take indexSize

    val block = indexReader.block

    EntryReader.parse(
      indexEntry = indexEntry,
      mightBeCompressed = block.hasPrefixCompression,
      valuesReader = valuesReader,
      indexOffset = positionBeforeRead,
      nextIndexOffset = nextIndexOffset,
      nextIndexSize = nextIndexSize,
      hasAccessPositionIndex = block.enableAccessPositionIndex,
      isNormalised = block.hasNormalisedBytes,
      previous = previous
    )
  }

  def readAll(keyValueCount: Int,
              sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
              valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
              addTo: Option[Slice[KeyValue.ReadOnly]] = None): Slice[KeyValue.ReadOnly] = {
    val readSortedIndexReader =
      sortedIndexReader
        .moveTo(0)
        .readAllAndGetReader()

    val keyValues = addTo getOrElse Slice.create[Persistent](keyValueCount)

    (1 to keyValueCount).foldLeft(Option.empty[Persistent]) {
      case (previousMayBe, _) =>
        val nextIndexSize =
          previousMayBe map {
            previous =>
              //If previous is known, keep reading same reader
              // and set the next position of the reader to be of the next index's offset.
              readSortedIndexReader moveTo previous.nextIndexOffset
              previous.nextIndexSize
          }

        val next =
          readKeyValue(
            indexEntrySizeMayBe = nextIndexSize,
            indexReader = readSortedIndexReader,
            overwriteNextIndexOffset = None,
            valuesReader = valuesReader,
            previous = previousMayBe
          )

        keyValues add next
        Some(next)
    }

    keyValues
  }

  def seekAndMatch(key: Slice[Byte],
                   startFrom: Option[Persistent],
                   sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                   valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]])(implicit order: KeyOrder[Slice[Byte]]): Option[Persistent] =
  //    if (startFrom.exists(from => order.gteq(from.key, key))) //TODO - to be removed via macros. this is for internal use only. Detects that a higher startFrom key does not get passed to this.
  //      IO.Left(swaydb.Error.Fatal("startFrom key is greater than target key."))
  //    else
    seekAndMatchToPersistent(
      matcher = KeyMatcher.Get(key),
      startFrom = startFrom,
      indexReader = sortedIndexReader,
      valuesReader = valuesReader
    )

  def matchOrSeek(key: Slice[Byte],
                  startFrom: Persistent,
                  sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                  valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]])(implicit ordering: KeyOrder[Slice[Byte]]): Option[Persistent] =
  //    if (ordering.gteq(startFrom.key, key)) //TODO - to be removed via macros. this is for internal use only. Detects that a higher startFrom key does not get passed to this.
  //      IO.Left(swaydb.Error.Fatal("startFrom key is greater than target key."))
  //    else
    SortedIndexBlock.matchOrSeek(
      matcher = KeyMatcher.Get(key),
      previous = startFrom,
      next = None,
      indexReader = sortedIndexReader,
      valuesReader = valuesReader
    ) match {
      case matched: KeyMatcher.Result.Matched =>
        Some(matched.result.toPersistent)

      case _: KeyMatcher.Result.AheadOrNoneOrEnd | _: KeyMatcher.Result.BehindStopped =>
        None
    }

  def searchSeekOne(key: Slice[Byte],
                    start: Persistent,
                    indexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                    valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]])(implicit order: KeyOrder[Slice[Byte]]): Option[Persistent] =
  //    if (order.gteq(start.key, key)) //TODO - to be removed via macros. this is for internal use only. Detects that a higher startFrom key does not get passed to this.
  //      IO.Left(swaydb.Error.Fatal("startFrom key is greater than target key."))
  //    else
    seekAndMatchToPersistent(
      matcher = KeyMatcher.Get.MatchOnly(key),
      startFrom = Some(start),
      indexReader = indexReader,
      valuesReader = valuesReader
    )

  def searchHigher(key: Slice[Byte],
                   startFrom: Option[Persistent],
                   sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                   valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]])(implicit order: KeyOrder[Slice[Byte]]): Option[Persistent] =
  //    if (startFrom.exists(from => order.gt(from.key, key))) //TODO - to be removed via macros. this is for internal use only. Detects that a higher startFrom key does not get passed to this.
  //      IO.Left(swaydb.Error.Fatal("startFrom key is greater than target key."))
  //    else
    seekAndMatchToPersistent(
      matcher = KeyMatcher.Higher(key),
      startFrom = startFrom,
      indexReader = sortedIndexReader,
      valuesReader = valuesReader
    )

  def matchOrSeekHigher(key: Slice[Byte],
                        startFrom: Option[Persistent],
                        sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                        valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]])(implicit order: KeyOrder[Slice[Byte]]): Option[Persistent] =
  //    if (startFrom.exists(from => order.gt(from.key, key))) //TODO - to be removed via macros. this is for internal use only. Detects that a higher startFrom key does not get passed to this.
  //      IO.Left(swaydb.Error.Fatal("startFrom key is greater than target key."))
  //    else
    startFrom match {
      case Some(lower) =>
        SortedIndexBlock.matchOrSeek(
          matcher = KeyMatcher.Higher(key),
          previous = lower,
          next = None,
          indexReader = sortedIndexReader,
          valuesReader = valuesReader
        ) match {
          case matched: KeyMatcher.Result.Matched =>
            Some(matched.result.toPersistent)

          case _: KeyMatcher.Result.AheadOrNoneOrEnd | _: KeyMatcher.Result.BehindStopped =>
            None
        }

      case None =>
        seekAndMatchToPersistent(
          matcher = KeyMatcher.Higher(key),
          startFrom = startFrom,
          indexReader = sortedIndexReader,
          valuesReader = valuesReader
        )
    }

  def searchHigherMatchOnly(key: Slice[Byte],
                            startFrom: Persistent,
                            sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                            valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]])(implicit order: KeyOrder[Slice[Byte]]): Option[Persistent] =
  //    if (order.gt(startFrom.key, key)) //TODO - to be removed via macros. this is for internal use only. Detects that a higher startFrom key does not get passed to this.
  //      IO.Left(swaydb.Error.Fatal("startFrom key is greater than target key."))
  //    else
    seekAndMatchToPersistent(
      matcher = KeyMatcher.Higher.MatchOnly(key),
      startFrom = Some(startFrom),
      indexReader = sortedIndexReader,
      valuesReader = valuesReader
    )

  def seekLowerAndMatch(key: Slice[Byte],
                        startFrom: Option[Persistent],
                        sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                        valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]])(implicit order: KeyOrder[Slice[Byte]]): Option[Persistent] =
  //    if (startFrom.exists(from => order.gteq(from.key, key))) //TODO - to be removed via macros. this is for internal use only. Detects that a higher startFrom key does not get passed to this.
  //      IO.Left(swaydb.Error.Fatal("startFrom key is greater than target key."))
  //    else
    seekAndMatchToPersistent(
      matcher = KeyMatcher.Lower(key),
      startFrom = startFrom,
      indexReader = sortedIndexReader,
      valuesReader = valuesReader
    )

  def matchOrSeekLower(key: Slice[Byte],
                       startFrom: Option[Persistent],
                       next: Option[Persistent],
                       sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                       valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]])(implicit order: KeyOrder[Slice[Byte]]): Option[Persistent] =
  //    if (startFrom.exists(from => order.gteq(from.key, key)) ||
  //      next.exists(next => next match {
  //        case range: Partial.RangeT => order.lt(range.toKey, key)
  //        case fixed: Partial.Fixed => order.lt(fixed.key, key)
  //      }
  //      )
  //    ) //TODO - to be removed via macros. this is for internal use only. Detects that a higher startFrom key does not get passed to this.
  //      IO.Left(swaydb.Error.Fatal("invalid startFrom or next."))
  //    else
    startFrom match {
      case Some(lower) =>
        SortedIndexBlock.matchOrSeek(
          matcher = KeyMatcher.Lower(key),
          previous = lower,
          next = next,
          indexReader = sortedIndexReader,
          valuesReader = valuesReader
        ) match {
          case matched: KeyMatcher.Result.Matched =>
            Some(matched.result.toPersistent)

          case _: KeyMatcher.Result.AheadOrNoneOrEnd | _: KeyMatcher.Result.BehindStopped =>
            None
        }

      case None =>
        next match {
          case Some(next) =>
            SortedIndexBlock.matchOrSeek(
              previous = next,
              next = None,
              matcher = KeyMatcher.Lower(key),
              indexReader = sortedIndexReader,
              valuesReader = valuesReader
            ) match {
              case matched: KeyMatcher.Result.Matched =>
                Some(matched.result.toPersistent)

              case _: KeyMatcher.Result.AheadOrNoneOrEnd | _: KeyMatcher.Result.BehindStopped =>
                None
            }

          case None =>
            seekAndMatchToPersistent(
              matcher = KeyMatcher.Lower(key),
              startFrom = startFrom,
              indexReader = sortedIndexReader,
              valuesReader = valuesReader
            )
        }
    }

  private def seekAndMatchToPersistent(matcher: KeyMatcher,
                                       startFrom: Option[Persistent],
                                       indexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                                       valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]]): Option[Persistent] =
    startFrom match {
      case Some(startFrom) =>
        seekAndMatchOrSeek(
          previous = startFrom,
          next = None,
          matcher = matcher,
          indexReader = indexReader,
          valuesReader = valuesReader
        ) match {
          case matched: KeyMatcher.Result.Matched =>
            Some(matched.result.toPersistent)

          case _: KeyMatcher.Result.AheadOrNoneOrEnd | _: KeyMatcher.Result.BehindStopped =>
            None
        }

      //No start from. Get the first index entry from the File and start from there.
      case None =>
        val keyValue =
          readKeyValue(
            fromPosition = 0,
            overwriteNextIndexOffset = None,
            indexReader = indexReader,
            valuesReader = valuesReader
          )

        matchOrSeekToPersistent(
          previous = keyValue,
          next = None,
          matcher = matcher,
          indexReader = indexReader,
          valuesReader = valuesReader
        )
    }

  def seekAndMatchOrSeek(matcher: KeyMatcher,
                         fromOffset: Int,
                         indexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                         valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]]): Result.Complete = {
    val persistent =
      readKeyValue(
        fromPosition = fromOffset,
        indexReader = indexReader,
        overwriteNextIndexOffset = None,
        valuesReader = valuesReader
      )

    //        ////////println("matchOrSeekAndPersistent")
    matchOrSeek(
      previous = persistent,
      next = None,
      matcher = matcher,
      indexReader = indexReader,
      valuesReader = valuesReader
    )
  }

  def seekAndMatchOrSeekToPersistent(matcher: KeyMatcher,
                                     fromOffset: Int,
                                     indexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                                     valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]]): Option[Persistent] = {
    val persistent =
      readKeyValue(
        fromPosition = fromOffset,
        indexReader = indexReader,
        overwriteNextIndexOffset = None,
        valuesReader = valuesReader
      )

    //        ////////println("matchOrSeekAndPersistent")
    matchOrSeekToPersistent(
      previous = persistent,
      next = None,
      matcher = matcher,
      indexReader = indexReader,
      valuesReader = valuesReader
    )
  }

  /**
   * Parse the input indexReader to a key-value and apply match.
   *
   * @param overwriteNextIndexOffset A full hashIndex stores it's offset within itself.
   *                                 This overwrites the [[Persistent.nextIndexOffset]]
   *                                 value in the parsed key-values which is required to
   *                                 perform sequential reads.
   */
  def readAndMatchToPersistent(matcher: KeyMatcher,
                               fromOffset: Int,
                               overwriteNextIndexOffset: Option[Int],
                               sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                               valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]]): Option[Persistent] =
    readAndMatch(
      matcher = matcher,
      fromOffset = fromOffset,
      overwriteNextIndexOffset = overwriteNextIndexOffset,
      sortedIndexReader = sortedIndexReader,
      valuesReader = valuesReader
    ) match {
      case matched: KeyMatcher.Result.Matched =>
        Some(matched.result.toPersistent)

      case _: Result.BehindStopped | _: Result.AheadOrNoneOrEnd | _: Result.BehindFetchNext =>
        None
    }

  def readAndMatch(matcher: KeyMatcher,
                   fromOffset: Int,
                   overwriteNextIndexOffset: Option[Int],
                   sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                   valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]]): KeyMatcher.Result = {
    val persistent =
      readKeyValue(
        fromPosition = fromOffset,
        indexReader = sortedIndexReader,
        overwriteNextIndexOffset = overwriteNextIndexOffset,
        valuesReader = valuesReader
      )

    matcher(
      previous = persistent,
      next = None,
      hasMore = hasMore(persistent)
    )
  }

  def read(fromOffset: Int,
           overwriteNextIndexOffset: Option[Int],
           sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
           valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]]): Persistent =
    readKeyValue(
      fromPosition = fromOffset,
      indexReader = sortedIndexReader,
      overwriteNextIndexOffset = overwriteNextIndexOffset,
      valuesReader = valuesReader
    )

  def readPreviousAndMatch(matcher: KeyMatcher,
                           next: Persistent,
                           fromOffset: Int,
                           overwriteNextIndexOffset: Option[Int],
                           sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                           valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]]): KeyMatcher.Result = {
    val persistent =
      readKeyValue(
        fromPosition = fromOffset,
        indexReader = sortedIndexReader,
        overwriteNextIndexOffset = overwriteNextIndexOffset,
        valuesReader = valuesReader
      )

    matcher(
      previous = persistent,
      next = Some(next),
      hasMore = hasMore(next)
    )
  }

  def readSeekAndMatch(matcher: KeyMatcher,
                       previous: Persistent,
                       fromOffset: Int,
                       overwriteNextIndexOffset: Option[Int],
                       sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                       valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]]): KeyMatcher.Result = {
    val persistent =
      readKeyValue(
        fromPosition = fromOffset,
        indexReader = sortedIndexReader,
        overwriteNextIndexOffset = overwriteNextIndexOffset,
        valuesReader = valuesReader
      )

    matcher(
      previous = previous,
      next = Some(persistent),
      hasMore = hasMore(persistent)
    )
  }

  def readNextKeyValue(previous: Persistent,
                       sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                       valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]]): Persistent =
    readKeyValue(
      previous = previous,
      indexReader = sortedIndexReader,
      valuesReader = valuesReader
    )

  def findAndMatchOrSeekMatch(matcher: KeyMatcher,
                              fromOffset: Int,
                              sortedIndex: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                              valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]]): KeyMatcher.Result = {
    val persistent =
      readKeyValue(
        fromPosition = fromOffset,
        indexReader = sortedIndex,
        overwriteNextIndexOffset = None,
        valuesReader = valuesReader
      )

    matchOrSeek(
      previous = persistent,
      next = None,
      matcher = matcher,
      indexReader = sortedIndex,
      valuesReader = valuesReader
    )
  }

  @tailrec
  def matchOrSeek(previous: Persistent,
                  next: Option[Persistent],
                  matcher: KeyMatcher,
                  indexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                  valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]]): KeyMatcher.Result.Complete =
    matcher(
      previous = previous,
      next = next,
      hasMore = hasMore(next getOrElse previous)
    ) match {
      case result: KeyMatcher.Result.Complete =>
        result

      case behind: KeyMatcher.Result.BehindFetchNext =>
        //        assert(previous.key.readInt() <= previousKeyValue.key.readInt())
        val readFrom = (next getOrElse behind.previous).toPersistent

        val nextNextKeyValue =
          readKeyValue(
            previous = readFrom,
            indexReader = indexReader,
            valuesReader = valuesReader
          )

        matchOrSeek(
          previous = readFrom,
          next = Some(nextNextKeyValue),
          matcher = matcher,
          indexReader = indexReader,
          valuesReader = valuesReader
        )
    }

  def seekAndMatchOrSeek(previous: Persistent,
                         next: Option[Persistent],
                         matcher: KeyMatcher,
                         indexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                         valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]]): KeyMatcher.Result.Complete = {
    val nextNextKeyValue =
      readKeyValue(
        previous = next getOrElse previous,
        indexReader = indexReader,
        valuesReader = valuesReader
      )

    matchOrSeek(
      previous = next getOrElse previous,
      next = Some(nextNextKeyValue),
      matcher = matcher,
      indexReader = indexReader,
      valuesReader = valuesReader
    )
  }

  def matchOrSeekToPersistent(previous: Persistent,
                              next: Option[Persistent],
                              matcher: KeyMatcher,
                              indexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                              valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]]): Option[Persistent] =
    matchOrSeek(
      previous = previous,
      next = next,
      matcher = matcher,
      indexReader = indexReader,
      valuesReader = valuesReader
    ) match {
      case matched: KeyMatcher.Result.Matched =>
        Some(matched.result.toPersistent)

      case _: KeyMatcher.Result.AheadOrNoneOrEnd | _: KeyMatcher.Result.BehindStopped =>
        None
    }

  /**
   * If key-value is read from copied HashIndex then keyValue.nextIndexSize can be 0 (unknown) so always
   * use nextIndexOffset to determine is there are more key-values.
   */
  private def hasMore(keyValue: Persistent) =
    keyValue.nextIndexOffset > -1
}

private[core] case class SortedIndexBlock(offset: SortedIndexBlock.Offset,
                                          enableAccessPositionIndex: Boolean,
                                          hasPrefixCompression: Boolean,
                                          normaliseForBinarySearch: Boolean,
                                          disableKeyPrefixCompression: Boolean,
                                          isPreNormalised: Boolean,
                                          headerSize: Int,
                                          segmentMaxIndexEntrySize: Int,
                                          compressionInfo: Option[Block.CompressionInfo]) extends Block[SortedIndexBlock.Offset] {
  val isNormalisedBinarySearchable =
    !hasPrefixCompression && (normaliseForBinarySearch || isPreNormalised)

  val isNonPreNormalised =
    !hasPrefixCompression && normaliseForBinarySearch && !isPreNormalised

  val hasNormalisedBytes =
    !isPreNormalised && normaliseForBinarySearch
}
