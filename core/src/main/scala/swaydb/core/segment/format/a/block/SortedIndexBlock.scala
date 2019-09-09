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
import swaydb.IO._
import swaydb.compression.CompressionInternal
import swaydb.core.data.{KeyValue, Persistent, Transient}
import swaydb.core.segment.format.a.block.KeyMatcher.Result
import swaydb.core.segment.format.a.block.reader.UnblockedReader
import swaydb.core.segment.format.a.entry.reader.SortedIndexEntryReader
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

    override def readBlock(header: Block.Header[Offset]): IO[swaydb.Error.Segment, SortedIndexBlock] =
      SortedIndexBlock.read(header)
  }

  object Config {
    val disabled =
      Config(
        ioStrategy = (dataType: IOAction) => IOStrategy.SynchronisedIO(cacheOnAccess = dataType.isCompressed),
        enableAccessPositionIndex = false,
        prefixCompressionResetCount = 0,
        enablePartialRead = false,
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
        enablePartialRead = enable.prefixCompression.disableKeyPrefixCompression && enable.prefixCompression.enablePartialRead,
        //cannot normalise if prefix compression is enabled.
        normaliseIndex = enable.prefixCompression.resetCount <= 0 && enable.prefixCompression.normaliseIndexForBinarySearch,
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
              enablePartialRead: Boolean,
              compressions: UncompressedBlockInfo => Seq[CompressionInternal]): Config =
      new Config(
        ioStrategy = ioStrategy,
        prefixCompressionResetCount = prefixCompressionResetCount max 0,
        enableAccessPositionIndex = enableAccessPositionIndex,
        //cannot normalise if prefix compression is enabled.
        normaliseIndex = prefixCompressionResetCount <= 0 && normaliseIndex,
        disableKeyPrefixCompression = enablePartialRead || disableKeyPrefixCompression,
        enablePartialRead = enablePartialRead,
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
                       val enablePartialRead: Boolean,
                       val compressions: UncompressedBlockInfo => Seq[CompressionInternal]) {

    def copy(ioStrategy: IOAction => IOStrategy = ioStrategy,
             prefixCompressionResetCount: Int = prefixCompressionResetCount,
             enableAccessPositionIndex: Boolean = enableAccessPositionIndex,
             normaliseIndex: Boolean = normaliseIndex,
             disableKeyPrefixCompression: Boolean = disableKeyPrefixCompression,
             enablePartialRead: Boolean = enablePartialRead,
             compressions: UncompressedBlockInfo => Seq[CompressionInternal] = compressions) =
    //do not use new here. Submit this to the apply function to that rules for creating the config gets applied.
      Config(
        ioStrategy = ioStrategy,
        prefixCompressionResetCount = prefixCompressionResetCount,
        disableKeyPrefixCompression = disableKeyPrefixCompression,
        enablePartialRead = enablePartialRead,
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
                   enablePartialRead: Boolean,
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
        ByteSizeOf.boolean + //enablePartialRead
        ByteSizeOf.varInt // segmentMaxSortedIndexEntrySize

    Bytes.sizeOf(size) + size
  }

  val noCompressionHeaderSize = {
    val size =
      Block.headerSize(false) +
        ByteSizeOf.boolean + //enablePositionIndex
        ByteSizeOf.boolean + //normalisedForBinarySearch
        ByteSizeOf.boolean + //hasPrefixCompression
        ByteSizeOf.boolean + //isPreNormalised
        ByteSizeOf.boolean + //disableKeyPrefixCompression
        ByteSizeOf.boolean + //enablePartialRead
        ByteSizeOf.varInt // segmentMaxSortedIndexEntrySize

    Bytes.sizeOf(size) + size
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

    (
      State(
        _bytes = bytes,
        headerSize = headSize,
        isPreNormalised = isPreNormalised,
        hasPrefixCompression = normalisedKeyValues.last.stats.hasPrefixCompression,
        enableAccessPositionIndex = normalisedKeyValues.last.sortedIndexConfig.enableAccessPositionIndex,
        disableKeyPrefixCompression = normalisedKeyValues.last.sortedIndexConfig.disableKeyPrefixCompression,
        enablePartialRead = normalisedKeyValues.last.sortedIndexConfig.enablePartialRead,
        normaliseIndex = normalisedKeyValues.last.sortedIndexConfig.normaliseIndex,
        segmentMaxIndexEntrySize = normalisedKeyValues.last.stats.segmentMaxSortedIndexEntrySize,
        compressions =
          //cannot have no compression to begin with a then have compression because that upsets the total bytes required.
          if (hasCompression)
            normalisedKeyValues.last.sortedIndexConfig.compressions
          else
            _ => Seq.empty
      ),
      normalisedKeyValues
    )
  }

  def write(keyValue: Transient, state: SortedIndexBlock.State): IO[swaydb.Error.Segment, Unit] =
    IO {
      state.bytes addAll keyValue.indexEntryBytes
    }

  def close(state: State): IO[swaydb.Error.Segment, State] =
    Block.block(
      headerSize = state.headerSize,
      bytes = state.bytes,
      compressions = state.compressions(UncompressedBlockInfo(state.bytes.size)),
      blockName = blockName
    ) flatMap {
      compressedOrUncompressedBytes =>
        state.bytes = compressedOrUncompressedBytes
        state.bytes addBoolean state.enableAccessPositionIndex
        state.bytes addBoolean state.hasPrefixCompression
        state.bytes addBoolean state.normaliseIndex
        state.bytes addBoolean state.isPreNormalised
        state.bytes addBoolean state.disableKeyPrefixCompression
        state.bytes addBoolean state.enablePartialRead
        state.bytes addIntUnsigned state.segmentMaxIndexEntrySize
        if (state.bytes.currentWritePosition > state.headerSize)
          IO.Left(swaydb.Error.Fatal(s"Calculated header size was incorrect. Expected: ${state.headerSize}. Used: ${state.bytes.currentWritePosition - 1}"))
        else
          IO.Right(state)
    }

  def read(header: Block.Header[SortedIndexBlock.Offset]): IO[swaydb.Error.Segment, SortedIndexBlock] =
    for {
      enableAccessPositionIndex <- header.headerReader.readBoolean()
      hasPrefixCompression <- header.headerReader.readBoolean()
      normaliseForBinarySearch <- header.headerReader.readBoolean()
      isPreNormalised <- header.headerReader.readBoolean()
      disableKeyPrefixCompression <- header.headerReader.readBoolean()
      enablePartialRead <- header.headerReader.readBoolean()
      segmentMaxIndexEntrySize <- header.headerReader.readIntUnsigned()
    } yield {
      SortedIndexBlock(
        offset = header.offset,
        enableAccessPositionIndex = enableAccessPositionIndex,
        hasPrefixCompression = hasPrefixCompression,
        normaliseForBinarySearch = normaliseForBinarySearch,
        segmentMaxIndexEntrySize = segmentMaxIndexEntrySize,
        disableKeyPrefixCompression = disableKeyPrefixCompression,
        enablePartialRead = enablePartialRead,
        isPreNormalised = isPreNormalised,
        headerSize = header.headerSize,
        compressionInfo = header.compressionInfo
      )
    }

  private def readNextKeyValue(previous: Persistent,
                               fullRead: Boolean,
                               indexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                               valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]]): IO[swaydb.Error.Segment, Persistent.Partial] =
    readNextKeyValue(
      indexEntrySizeMayBe = Some(previous.nextIndexSize),
      overwriteNextIndexOffset = None,
      fullRead = fullRead,
      indexReader = indexReader moveTo previous.nextIndexOffset,
      valuesReader = valuesReader,
      previous = Some(previous)
    )

  private def readNextKeyValue(fromPosition: Int,
                               overwriteNextIndexOffset: Option[Int],
                               fullRead: Boolean,
                               indexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                               valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]]): IO[swaydb.Error.Segment, Persistent.Partial] =
    readNextKeyValue(
      indexEntrySizeMayBe = None,
      fullRead = fullRead,
      overwriteNextIndexOffset = overwriteNextIndexOffset,
      indexReader = indexReader moveTo fromPosition,
      valuesReader = valuesReader,
      previous = None
    )

  /**
   * Pre-requisite: The position of the index on the reader should be set.
   *
   * @param overwriteNextIndexOffset HashIndex with full copy store nextIndexOffset within themselves.
   *                                 This param overwrites calculation of nextIndexOffset from indexReader
   *                                 and applies this value if set. nextIndexOffset is used to
   *                                 calculate if the next key-value exists or if the the currently read
   *                                 key-value is in sequence with next.
   */
  private def readNextKeyValue(indexEntrySizeMayBe: Option[Int],
                               overwriteNextIndexOffset: Option[Int],
                               fullRead: Boolean,
                               indexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                               valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
                               previous: Option[Persistent]): IO[swaydb.Error.Segment, Persistent.Partial] =
    try {
      val positionBeforeRead = indexReader.getPosition
      //size of the index entry to read
      //todo read indexReader.block.segmentMaxIndexEntrySize in one seek.
      val indexSize =
      indexEntrySizeMayBe match {
        case Some(indexEntrySize) if indexEntrySize > 0 =>
          indexReader skip Bytes.sizeOf(indexEntrySize)
          indexEntrySize

        case None | Some(_) =>
          indexReader.readIntUnsigned().get
      }

      //5 extra bytes are read for each entry to fetch the next index's size.
      val bytesToRead = indexSize + ByteSizeOf.varInt

      //read all bytes for this index entry plus the next 5 bytes to fetch next index entry's size.
      val indexEntryBytesAndNextIndexEntrySize = (indexReader read bytesToRead).get

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
        indexEntryBytesAndNextIndexEntrySize
          .drop(indexSize)
          .readIntUnsigned()
          .map {
            nextIndexEntrySize =>
              (nextIndexEntrySize, indexReader.getPosition - extraBytesRead)
          }.get
      }

      //take only the bytes required for this in entry and submit it for parsing/reading.
      val indexEntry = indexEntryBytesAndNextIndexEntrySize take indexSize

      if (indexReader.block.enablePartialRead)
        if (fullRead)
          SortedIndexEntryReader.fullReadFromPartial(
            indexEntry = indexEntry,
            mightBeCompressed = indexReader.block.hasPrefixCompression,
            valuesReader = valuesReader,
            indexOffset = positionBeforeRead,
            nextIndexOffset = nextIndexOffset,
            nextIndexSize = nextIndexSize,
            hasAccessPositionIndex = indexReader.block.enableAccessPositionIndex,
            isNormalised = indexReader.block.hasNormalisedBytes,
            isPartialReadEnabled = indexReader.block.enablePartialRead,
            previous = previous
          )
        else
          SortedIndexEntryReader.partialRead(
            indexEntry = indexEntry,
            block = indexReader.block,
            indexOffset = positionBeforeRead,
            nextIndexOffset = nextIndexOffset,
            nextIndexSize = nextIndexSize,
            valuesReader = valuesReader,
            previous = previous
          )
      else
        SortedIndexEntryReader.fullRead(
          indexEntry = indexEntry,
          mightBeCompressed = indexReader.block.hasPrefixCompression,
          valuesReader = valuesReader,
          indexOffset = positionBeforeRead,
          nextIndexOffset = nextIndexOffset,
          nextIndexSize = nextIndexSize,
          hasAccessPositionIndex = indexReader.block.enableAccessPositionIndex,
          isNormalised = indexReader.block.hasNormalisedBytes,
          isPartialReadEnabled = indexReader.block.enablePartialRead,
          previous = previous
        )

      //      IO.Right {
      //        Persistent.Put(
      //          _key = indexEntry.take(indexSize).takeRight(4),
      //          deadline = None,
      //          valueCache = null,
      //          _time = Time.empty,
      //          nextIndexOffset = nextIndexOffset,
      //          nextIndexSize = nextIndexSize,
      //          indexOffset = positionBeforeRead,
      //          valueOffset = 0,
      //          valueLength = 0,
      //          accessPosition = 0,
      //          isPrefixCompressed = false
      //        )
      //      }
    } catch {
      case exception: Exception =>
        IO.failed(exception)
    }

  def readAll(keyValueCount: Int,
              sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
              valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
              addTo: Option[Slice[KeyValue.ReadOnly]] = None): IO[swaydb.Error.Segment, Slice[KeyValue.ReadOnly]] =
    try {
      sortedIndexReader moveTo 0
      val readSortedIndexReader = sortedIndexReader.readAllAndGetReader().get

      val entries = addTo getOrElse Slice.create[Persistent](keyValueCount)
      (1 to keyValueCount).foldLeftIO(Option.empty[Persistent]) {
        case (previousMayBe, _) =>
          val nextIndexSize =
            previousMayBe map {
              previous =>
                //If previous is known, keep reading same reader
                // and set the next position of the reader to be of the next index's offset.
                readSortedIndexReader moveTo previous.nextIndexOffset
                previous.nextIndexSize
            }

          readNextKeyValue(
            indexEntrySizeMayBe = nextIndexSize,
            indexReader = readSortedIndexReader,
            overwriteNextIndexOffset = None,
            fullRead = true,
            valuesReader = valuesReader,
            previous = previousMayBe
          ) flatMap {
            next =>
              next.toPersistent map {
                next =>
                  entries add next
                  Some(next)
              }
          }
      } map (_ => entries)
    } catch {
      case exception: Exception =>
        IO.failed(exception)
    }

  def search(key: Slice[Byte],
             startFrom: Option[Persistent.Partial],
             fullRead: Boolean,
             sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
             valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]])(implicit order: KeyOrder[Slice[Byte]]): IO[swaydb.Error.Segment, Option[Persistent.Partial]] =
  //    if (startFrom.exists(from => order.gt(from.key, key))) //TODO - to be removed via macros. this is for internal use only. Detects that a higher startFrom key does not get passed to this.
  //      IO.Left(swaydb.Error.Fatal("startFrom key is greater than target key."))
  //    else
    search(
      matcher = KeyMatcher.Get(key),
      startFrom = startFrom,
      fullRead = fullRead,
      indexReader = sortedIndexReader,
      valuesReader = valuesReader
    )

  def searchSeekOne(key: Slice[Byte],
                    start: Persistent.Partial,
                    fullRead: Boolean,
                    indexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                    valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]])(implicit order: KeyOrder[Slice[Byte]]): IO[swaydb.Error.Segment, Option[Persistent.Partial]] =
  //    if (order.gt(start.key, key)) //TODO - to be removed via macros. this is for internal use only. Detects that a higher startFrom key does not get passed to this.
  //      IO.Left(swaydb.Error.Fatal("startFrom key is greater than target key."))
  //    else
    search(
      matcher = KeyMatcher.Get.SeekOne(key),
      startFrom = Some(start),
      fullRead = fullRead,
      indexReader = indexReader,
      valuesReader = valuesReader
    )

  def searchHigher(key: Slice[Byte],
                   startFrom: Option[Persistent.Partial],
                   fullRead: Boolean,
                   sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                   valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]])(implicit order: KeyOrder[Slice[Byte]]): IO[swaydb.Error.Segment, Option[Persistent.Partial]] =
  //    if (startFrom.exists(from => order.gt(from.key, key))) //TODO - to be removed via macros. this is for internal use only. Detects that a higher startFrom key does not get passed to this.
  //      IO.Left(swaydb.Error.Fatal("startFrom key is greater than target key."))
  //    else
    search(
      matcher = KeyMatcher.Higher(key),
      startFrom = startFrom,
      fullRead = fullRead,
      indexReader = sortedIndexReader,
      valuesReader = valuesReader
    )

  def searchHigherSeekOne(key: Slice[Byte],
                          startFrom: Persistent.Partial,
                          fullRead: Boolean,
                          sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                          valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]])(implicit order: KeyOrder[Slice[Byte]]): IO[swaydb.Error.Segment, Option[Persistent.Partial]] =
  //    if (order.gt(startFrom.key, key)) //TODO - to be removed via macros. this is for internal use only. Detects that a higher startFrom key does not get passed to this.
  //      IO.Left(swaydb.Error.Fatal("startFrom key is greater than target key."))
  //    else
    search(
      matcher = KeyMatcher.Higher.SeekOne(key),
      startFrom = Some(startFrom),
      fullRead = fullRead,
      indexReader = sortedIndexReader,
      valuesReader = valuesReader
    )

  def searchLower(key: Slice[Byte],
                  startFrom: Option[Persistent.Partial],
                  fullRead: Boolean,
                  sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                  valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]])(implicit order: KeyOrder[Slice[Byte]]): IO[swaydb.Error.Segment, Option[Persistent.Partial]] =
  //    if (startFrom.exists(from => order.gt(from.key, key))) //TODO - to be removed via macros. this is for internal use only. Detects that a higher startFrom key does not get passed to this.
  //      IO.Left(swaydb.Error.Fatal("startFrom key is greater than target key."))
  //    else
    search(
      matcher = KeyMatcher.Lower(key),
      startFrom = startFrom,
      fullRead = fullRead,
      indexReader = sortedIndexReader,
      valuesReader = valuesReader
    )

  private def search(matcher: KeyMatcher,
                     startFrom: Option[Persistent.Partial],
                     fullRead: Boolean,
                     indexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                     valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]]): IO[swaydb.Error.Segment, Option[Persistent.Partial]] =
    startFrom match {
      case Some(startFrom) =>
        matchOrNextAndPersistent(
          previous = startFrom,
          next = None,
          fullRead = fullRead,
          matcher = matcher,
          indexReader = indexReader,
          valuesReader = valuesReader
        )

      //No start from. Get the first index entry from the File and start from there.
      case None =>
        readNextKeyValue(
          fromPosition = 0,
          indexReader = indexReader,
          fullRead = fullRead,
          overwriteNextIndexOffset = None,
          valuesReader = valuesReader
        ) flatMap {
          keyValue =>
            matchOrNextAndPersistent(
              previous = keyValue,
              next = None,
              fullRead = fullRead,
              matcher = matcher,
              indexReader = indexReader,
              valuesReader = valuesReader
            )
        }
    }

  def findAndMatchOrNextPersistent(matcher: KeyMatcher,
                                   fromOffset: Int,
                                   fullRead: Boolean,
                                   indexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                                   valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]]): IO[swaydb.Error.Segment, Option[Persistent.Partial]] =
    readNextKeyValue(
      fromPosition = fromOffset,
      indexReader = indexReader,
      fullRead = fullRead,
      overwriteNextIndexOffset = None,
      valuesReader = valuesReader
    ) flatMap {
      persistent =>
        //        println("matchOrNextAndPersistent")
        matchOrNextAndPersistent(
          previous = persistent,
          next = None,
          fullRead = fullRead,
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
  def parseAndMatch(matcher: KeyMatcher,
                    fromOffset: Int,
                    fullRead: Boolean,
                    overwriteNextIndexOffset: Option[Int],
                    indexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                    valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]]): IO[swaydb.Error.Segment, Option[Persistent.Partial]] =
    readNextKeyValue(
      fromPosition = fromOffset,
      indexReader = indexReader,
      fullRead = fullRead,
      overwriteNextIndexOffset = overwriteNextIndexOffset,
      valuesReader = valuesReader
    ) map {
      persistent =>
        matcher(
          previous = persistent,
          next = None,
          hasMore = hasMore(persistent)
        ) match {
          case Result.Matched(_, result, _) =>
            Some(result)

          case Result.BehindStopped(_) | Result.AheadOrNoneOrEnd | Result.BehindFetchNext(_) =>
            None
        }
    }

  def findAndMatchOrNextMatch(matcher: KeyMatcher,
                              fromOffset: Int,
                              fullRead: Boolean,
                              sortedIndex: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                              valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]]): IO[swaydb.Error.Segment, KeyMatcher.Result] =
    readNextKeyValue(
      fromPosition = fromOffset,
      indexReader = sortedIndex,
      fullRead = fullRead,
      overwriteNextIndexOffset = None,
      valuesReader = valuesReader
    ) flatMap {
      persistent =>
        matchOrNext(
          previous = persistent,
          next = None,
          fullRead = fullRead,
          matcher = matcher,
          indexReader = sortedIndex,
          valuesReader = valuesReader
        )
    }

  @tailrec
  def matchOrNext(previous: Persistent.Partial,
                  next: Option[Persistent.Partial],
                  fullRead: Boolean,
                  matcher: KeyMatcher,
                  indexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                  valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]]): IO[swaydb.Error.Segment, KeyMatcher.Result.Complete] =
    matcher(
      previous = previous,
      next = next,
      hasMore = hasMore(next getOrElse previous)
    ) match {
      case result: KeyMatcher.Result.Complete =>
        IO.Right(result)

      case KeyMatcher.Result.BehindFetchNext(previousKeyValue) =>
        //        assert(previous.key.readInt() <= previousKeyValue.key.readInt())
        (next getOrElse previousKeyValue).toPersistent match {
          case IO.Right(readFrom) =>
            readNextKeyValue(
              previous = readFrom,
              fullRead = fullRead,
              indexReader = indexReader,
              valuesReader = valuesReader
            ) match {
              case IO.Right(nextNextKeyValue) =>
                matchOrNext(
                  previous = readFrom,
                  next = Some(nextNextKeyValue),
                  matcher = matcher,
                  fullRead = fullRead,
                  indexReader = indexReader,
                  valuesReader = valuesReader
                )

              case IO.Left(error) =>
                IO.Left(error)
            }

          case IO.Left(value) =>
            IO.Left(value)
        }
    }

  def matchOrNextAndPersistent(previous: Persistent.Partial,
                               next: Option[Persistent.Partial],
                               matcher: KeyMatcher,
                               fullRead: Boolean,
                               indexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                               valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]]): IO[swaydb.Error.Segment, Option[Persistent.Partial]] =
    matchOrNext(
      previous = previous,
      next = next,
      matcher = matcher,
      fullRead = fullRead,
      indexReader = indexReader,
      valuesReader = valuesReader
    ) map {
      case KeyMatcher.Result.Matched(_, keyValue, _) =>
        Some(keyValue)

      case KeyMatcher.Result.AheadOrNoneOrEnd | _: KeyMatcher.Result.BehindStopped =>
        None
    }

  /**
   * If key-value is read from copied HashIndex then keyValue.nextIndexSize can be 0 (unknown) so always
   * use nextIndexOffset to determine is there are more key-values.
   */
  private def hasMore(keyValue: Persistent.Partial) =
    keyValue.nextIndexOffset > -1
}

private[core] case class SortedIndexBlock(offset: SortedIndexBlock.Offset,
                                          enableAccessPositionIndex: Boolean,
                                          hasPrefixCompression: Boolean,
                                          normaliseForBinarySearch: Boolean,
                                          disableKeyPrefixCompression: Boolean,
                                          enablePartialRead: Boolean,
                                          isPreNormalised: Boolean,
                                          headerSize: Int,
                                          segmentMaxIndexEntrySize: Int,
                                          compressionInfo: Option[Block.CompressionInfo]) extends Block[SortedIndexBlock.Offset] {
  val isBinarySearchable =
    !hasPrefixCompression && (normaliseForBinarySearch || isPreNormalised)

  val hasNormalisedBytes =
    !isPreNormalised && normaliseForBinarySearch
}
