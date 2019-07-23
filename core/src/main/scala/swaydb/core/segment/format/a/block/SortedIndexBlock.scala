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
import swaydb.IO
import swaydb.compression.CompressionInternal
import swaydb.core.data.{KeyValue, Persistent, Transient}
import swaydb.core.io.reader.Reader
import swaydb.core.segment.SegmentException.SegmentCorruptionException
import swaydb.core.segment.format.a.block.reader.UnblockedReader
import swaydb.core.segment.format.a.entry.reader.EntryReader
import swaydb.core.util.cache.Cache
import swaydb.core.util.{Bytes, FunctionUtil}
import swaydb.IO._
import swaydb.data.config.{IOAction, IOStrategy, UncompressedBlockInfo}
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.data.util.ByteSizeOf

import scala.annotation.tailrec

private[core] object SortedIndexBlock extends LazyLogging {

  val blockName = this.getClass.getSimpleName.dropRight(1)

  implicit object SortedIndexBlockOps extends BlockOps[SortedIndexBlock.Offset, SortedIndexBlock] {
    override def updateBlockOffset(block: SortedIndexBlock, start: Int, size: Int): SortedIndexBlock =
      block.copy(offset = createOffset(start = start, size = size))

    override def createOffset(start: Int, size: Int): Offset =
      SortedIndexBlock.Offset(start = start, size = size)

    override def readBlock(header: Block.Header[Offset]): IO[SortedIndexBlock] =
      SortedIndexBlock.read(header)
  }

  object Config {
    val disabled =
      Config(
        blockIO = dataType => IOStrategy.SynchronisedIO(cacheOnAccess = dataType.isCompressed),
        enableAccessPositionIndex = false,
        prefixCompressionResetCount = 0,
        compressions = _ => Seq.empty
      )

    def apply(config: swaydb.data.config.SortedKeyIndex): Config =
      config match {
        case config: swaydb.data.config.SortedKeyIndex.Enable =>
          apply(config)
      }

    def apply(enable: swaydb.data.config.SortedKeyIndex.Enable): Config =
      Config(
        enableAccessPositionIndex = enable.enablePositionIndex,
        prefixCompressionResetCount = enable.prefixCompression.toOption.flatMap(_.resetCount).getOrElse(0),
        blockIO = FunctionUtil.safe(IOStrategy.defaultSynchronisedStoredIfCompressed, enable.ioStrategy),
        compressions =
          FunctionUtil.safe(
            default = _ => Seq.empty[CompressionInternal],
            function = enable.compressions(_) map CompressionInternal.apply
          )
      )
  }

  case class Config(blockIO: IOAction => IOStrategy,
                    prefixCompressionResetCount: Int,
                    enableAccessPositionIndex: Boolean,
                    compressions: UncompressedBlockInfo => Seq[CompressionInternal])

  case class Offset(start: Int, size: Int) extends BlockOffset

  case class State(var _bytes: Slice[Byte],
                   headerSize: Int,
                   hasPrefixCompression: Boolean,
                   enableAccessPositionIndex: Boolean,
                   compressions: UncompressedBlockInfo => Seq[CompressionInternal]) {
    def bytes = _bytes

    def bytes_=(bytes: Slice[Byte]) =
      this._bytes = bytes
  }

  val hasCompressionHeaderSize = {
    val size =
      Block.headerSize(true) +
        ByteSizeOf.boolean + //enablePositionIndex
        ByteSizeOf.boolean //hasPrefixCompression

    Bytes.sizeOf(size) + size
  }

  val noCompressionHeaderSize = {
    val size =
      Block.headerSize(false) +
        ByteSizeOf.boolean + //enablePositionIndex
        ByteSizeOf.boolean //hasPrefixCompression

    Bytes.sizeOf(size) + size
  }

  def headerSize(hasCompression: Boolean): Int =
    if (hasCompression)
      hasCompressionHeaderSize
    else
      noCompressionHeaderSize

  def init(keyValues: Iterable[Transient]): SortedIndexBlock.State = {
    val hasCompression = keyValues.last.sortedIndexConfig.compressions(UncompressedBlockInfo(keyValues.last.stats.segmentSortedIndexSize)).nonEmpty
    val headSize = headerSize(hasCompression)
    val bytes =
      if (hasCompression) //stats calculate size with no compression if there is compression add remaining bytes.
        Slice.create[Byte](keyValues.last.stats.segmentSortedIndexSize - noCompressionHeaderSize + headSize)
      else
        Slice.create[Byte](keyValues.last.stats.segmentSortedIndexSize)

    bytes moveWritePosition headSize
    State(
      _bytes = bytes,
      headerSize = headSize,
      enableAccessPositionIndex = keyValues.last.sortedIndexConfig.enableAccessPositionIndex,
      hasPrefixCompression = keyValues.last.stats.hasPrefixCompression,
      compressions =
        //cannot have no compression to begin with a then have compression because that upsets the total bytes required.
        if (hasCompression)
          keyValues.last.sortedIndexConfig.compressions
        else
          _ => Seq.empty
    )
  }

  def write(keyValue: Transient, state: SortedIndexBlock.State): IO[Unit] =
    IO {
      if (state.enableAccessPositionIndex) {
        state.bytes addIntUnsigned (keyValue.indexEntryBytes.size + keyValue.stats.thisKeyValueAccessIndexPositionByteSize)
        state.bytes addIntUnsigned keyValue.stats.thisKeyValueAccessIndexPosition
        state.bytes addAll keyValue.indexEntryBytes
      } else {
        state.bytes addIntUnsigned keyValue.indexEntryBytes.size
        state.bytes addAll keyValue.indexEntryBytes
      }
    }

  def close(state: State): IO[State] =
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
        if (state.bytes.currentWritePosition > state.headerSize)
          IO.Failure(IO.Error.Fatal(s"Calculated header size was incorrect. Expected: ${state.headerSize}. Used: ${state.bytes.currentWritePosition - 1}"))
        else
          IO.Success(state)
    }

  def read(header: Block.Header[SortedIndexBlock.Offset]): IO[SortedIndexBlock] =
    for {
      enableAccessPositionIndex <- header.headerReader.readBoolean()
      hasPrefixCompression <- header.headerReader.readBoolean()
    } yield {
      SortedIndexBlock(
        offset = header.offset,
        enableAccessPositionIndex = enableAccessPositionIndex,
        hasPrefixCompression = hasPrefixCompression,
        headerSize = header.headerSize,
        compressionInfo = header.compressionInfo
      )
    }

  private def readNextKeyValue(previous: Persistent,
                               indexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                               valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]]): IO[Persistent] =
    readNextKeyValue(
      indexEntrySizeMayBe = Some(previous.nextIndexSize),
      indexReader = indexReader moveTo previous.nextIndexOffset,
      valuesReader = valuesReader,
      previous = Some(previous)
    )

  private def readNextKeyValue(fromPosition: Int,
                               indexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                               valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]]): IO[Persistent] =
    readNextKeyValue(
      indexEntrySizeMayBe = None,
      indexReader = indexReader moveTo fromPosition,
      valuesReader = valuesReader,
      previous = None
    )

  //Pre-requisite: The position of the index on the reader should be set.
  private def readNextKeyValue(indexEntrySizeMayBe: Option[Int],
                               indexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                               valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
                               previous: Option[Persistent]): IO[Persistent] =
    try {
      val positionBeforeRead = indexReader.getPosition
      //size of the index entry to read
      val indexSize =
        indexEntrySizeMayBe match {
          case Some(indexEntrySize) =>
            indexReader skip Bytes.sizeOf(indexEntrySize)
            indexEntrySize

          case None =>
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
        (0, -1)
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

      val sortedIndexReader = Reader(indexEntryBytesAndNextIndexEntrySize.take(indexSize))

      val accessPosition =
        if (indexReader.block.enableAccessPositionIndex)
          sortedIndexReader.readIntUnsigned().get
        else
          0

      //create value cache reader given the value offset.
      //todo pass in blockIO config when read values.
      def valueCache =
        valuesReader map {
          valuesReader =>
            Cache.concurrentIO[ValuesBlock.Offset, UnblockedReader[ValuesBlock.Offset, ValuesBlock]](synchronised = false, stored = false) {
              offset =>
                if (offset.size == 0)
                  ValuesBlock.emptyUnblockedIO
                else
                  IO(UnblockedReader.moveTo(offset, valuesReader))
            }
        }

      EntryReader.read(
        //take only the bytes required for this in entry and submit it for parsing/reading.
        indexReader = sortedIndexReader,
        mightBeCompressed = indexReader.block.hasPrefixCompression,
        valueCache = valueCache,
        indexOffset = positionBeforeRead,
        nextIndexOffset = nextIndexOffset,
        nextIndexSize = nextIndexSize,
        accessPosition = accessPosition,
        previous = previous
      )
    } catch {
      case exception: Exception =>
        exception match {
          case _: ArrayIndexOutOfBoundsException | _: IndexOutOfBoundsException | _: IllegalArgumentException | _: NegativeArraySizeException =>
            val atPosition: String = indexEntrySizeMayBe.map(size => s" of size $size") getOrElse ""
            IO.Failure(
              IO.Error.Fatal(
                SegmentCorruptionException(
                  message = s"Corrupted Segment: Failed to read index entry at reader position ${indexReader.getPosition} - $atPosition}",
                  cause = exception
                )
              )
            )

          case ex: Exception =>
            IO.Failure(ex)
        }
    }

  def readAll(keyValueCount: Int,
              sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
              valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
              addTo: Option[Slice[KeyValue.ReadOnly]] = None): IO[Slice[KeyValue.ReadOnly]] =
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
            valuesReader = valuesReader,
            previous = previousMayBe
          ) map {
            next =>
              entries add next
              Some(next)
          }
      } map (_ => entries)
    } catch {
      case exception: Exception =>
        exception match {
          case _: ArrayIndexOutOfBoundsException | _: IndexOutOfBoundsException | _: IllegalArgumentException | _: NegativeArraySizeException =>
            IO.Failure(
              IO.Error.Fatal(
                SegmentCorruptionException(
                  message = s"Corrupted Segment: Failed to read index bytes",
                  cause = exception
                )
              )
            )

          case ex: Exception =>
            IO.Failure(ex)
        }
    }

  def search(key: Slice[Byte],
             startFrom: Option[Persistent],
             indexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
             valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]])(implicit order: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] =
    if (startFrom.exists(from => order.gt(from.key, key))) //TODO - to be removed via macros. this is for internal use only. Detects that a higher startFrom key does not get passed to this.
      IO.Failure(IO.Error.Fatal("startFrom key is greater than target key."))
    else
      search(
        matcher = KeyMatcher.Get(key),
        startFrom = startFrom,
        indexReader = indexReader,
        valuesReader = valuesReader
      )

  def searchHigher(key: Slice[Byte],
                   startFrom: Option[Persistent],
                   sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                   valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]])(implicit order: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] =
    if (startFrom.exists(from => order.gt(from.key, key))) //TODO - to be removed via macros. this is for internal use only. Detects that a higher startFrom key does not get passed to this.
      IO.Failure(IO.Error.Fatal("startFrom key is greater than target key."))
    else
      search(
        matcher = KeyMatcher.Higher(key),
        startFrom = startFrom,
        indexReader = sortedIndexReader,
        valuesReader = valuesReader
      )

  def searchHigherSeekOne(key: Slice[Byte],
                          startFrom: Persistent,
                          indexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                          valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]])(implicit order: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] =
    if (order.gt(startFrom.key, key)) //TODO - to be removed via macros. this is for internal use only. Detects that a higher startFrom key does not get passed to this.
      IO.Failure(IO.Error.Fatal("startFrom key is greater than target key."))
    else
      search(
        matcher = KeyMatcher.Higher.SeekOne(key),
        startFrom = Some(startFrom),
        indexReader = indexReader,
        valuesReader = valuesReader
      )

  def searchLower(key: Slice[Byte],
                  startFrom: Option[Persistent],
                  indexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                  valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]])(implicit order: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] =
    if (startFrom.exists(from => order.gt(from.key, key))) //TODO - to be removed via macros. this is for internal use only. Detects that a higher startFrom key does not get passed to this.
      IO.Failure(IO.Error.Fatal("startFrom key is greater than target key."))
    else
      search(
        matcher = KeyMatcher.Lower(key),
        startFrom = startFrom,
        indexReader = indexReader,
        valuesReader = valuesReader
      )

  private def search(matcher: KeyMatcher,
                     startFrom: Option[Persistent],
                     indexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                     valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]]): IO[Option[Persistent]] =
    startFrom match {
      case Some(startFrom) =>
        matchOrNextAndPersistent(
          previous = startFrom,
          next = None,
          matcher = matcher,
          indexReader = indexReader,
          valuesReader = valuesReader
        )

      //No start from. Get the first index entry from the File and start from there.
      case None =>
        readNextKeyValue(
          fromPosition = 0,
          indexReader = indexReader,
          valuesReader = valuesReader
        ) flatMap {
          keyValue =>
            matchOrNextAndPersistent(
              previous = keyValue,
              next = None,
              matcher = matcher,
              indexReader = indexReader,
              valuesReader = valuesReader
            )
        }
    }

  def findAndMatchOrNextPersistent(matcher: KeyMatcher,
                                   fromOffset: Int,
                                   indexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                                   valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]]): IO[Option[Persistent]] =
    readNextKeyValue(
      fromPosition = fromOffset,
      indexReader = indexReader,
      valuesReader = valuesReader
    ) flatMap {
      persistent =>
        matchOrNextAndPersistent(
          previous = persistent,
          next = None,
          matcher = matcher,
          indexReader = indexReader,
          valuesReader = valuesReader
        )
    }

  def findAndMatchOrNextMatch(matcher: KeyMatcher,
                              fromOffset: Int,
                              sortedIndex: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                              valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]]): IO[KeyMatcher.Result] =
    readNextKeyValue(
      fromPosition = fromOffset,
      indexReader = sortedIndex,
      valuesReader = valuesReader
    ) flatMap {
      persistent =>
        matchOrNext(
          previous = persistent,
          next = None,
          matcher = matcher,
          indexReader = sortedIndex,
          valuesReader = valuesReader
        )
    }

  @tailrec
  def matchOrNext(previous: Persistent,
                  next: Option[Persistent],
                  matcher: KeyMatcher,
                  indexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                  valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]]): IO[KeyMatcher.Result.Complete] =
    matcher(
      previous = previous,
      next = next,
      hasMore = hasMore(next getOrElse previous)
    ) match {
      case KeyMatcher.Result.BehindFetchNext(previousKeyValue) =>
//        assert(previous.key.readInt() <= previousKeyValue.key.readInt())
        val readFrom = next getOrElse previousKeyValue
        readNextKeyValue(
          previous = readFrom,
          indexReader = indexReader,
          valuesReader = valuesReader
        ) match {
          case IO.Success(nextNextKeyValue) =>
            matchOrNext(
              previous = readFrom,
              next = Some(nextNextKeyValue),
              matcher = matcher,
              indexReader = indexReader,
              valuesReader = valuesReader
            )

          case IO.Failure(exception) =>
            IO.Failure(exception)
        }

      case result: KeyMatcher.Result.Complete =>
        result.asIO
    }

  def matchOrNextAndPersistent(previous: Persistent,
                               next: Option[Persistent],
                               matcher: KeyMatcher,
                               indexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                               valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]]): IO[Option[Persistent]] =
    matchOrNext(
      previous = previous,
      next = next,
      matcher = matcher,
      indexReader = indexReader,
      valuesReader = valuesReader
    ) match {
      case IO.Success(KeyMatcher.Result.Matched(_, keyValue, _)) =>
        IO.Success(Some(keyValue))

      case IO.Success(KeyMatcher.Result.AheadOrNoneOrEnd | _: KeyMatcher.Result.BehindStopped) =>
        IO.none

      case IO.Failure(error) =>
        IO.Failure(error)
    }

  private def hasMore(keyValue: Persistent) =
    keyValue.nextIndexSize > 0
}

private[core] case class SortedIndexBlock(offset: SortedIndexBlock.Offset,
                                          enableAccessPositionIndex: Boolean,
                                          hasPrefixCompression: Boolean,
                                          headerSize: Int,
                                          compressionInfo: Option[Block.CompressionInfo]) extends Block[SortedIndexBlock.Offset]
