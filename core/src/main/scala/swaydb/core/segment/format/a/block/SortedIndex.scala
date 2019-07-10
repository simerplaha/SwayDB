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
import swaydb.core.data.{KeyValue, Persistent}
import swaydb.core.io.reader.{BlockReader, Reader}
import swaydb.core.segment.SegmentException.SegmentCorruptionException
import swaydb.core.segment.format.a.entry.reader.EntryReader
import swaydb.core.segment.format.a.{KeyMatcher, OffsetBase}
import swaydb.core.util.Bytes
import swaydb.data.IO
import swaydb.data.IO._
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.data.util.ByteSizeOf

import scala.annotation.tailrec

private[core] object SortedIndex {

  val blockName = this.getClass.getSimpleName.dropRight(1)

  object Config {
    val disabled =
      Config(
        cacheOnAccess = false,
        enableAccessPositionIndex = false,
        prefixCompressionResetCount = 0,
        compressions = Seq.empty
      )

    def apply(config: swaydb.data.config.SortedKeyIndex): Config =
      config match {
        case config: swaydb.data.config.SortedKeyIndex.Enable =>
          apply(config)
      }

    def apply(config: swaydb.data.config.SortedKeyIndex.Enable): Config =
      Config(
        cacheOnAccess = config.cacheOnAccess,
        enableAccessPositionIndex = config.enablePositionIndex,
        prefixCompressionResetCount = config.prefixCompression.toOption.flatMap(_.resetCount).getOrElse(0),
        compressions = config.compression map CompressionInternal.apply
      )
  }

  case class Config(cacheOnAccess: Boolean,
                    prefixCompressionResetCount: Int,
                    enableAccessPositionIndex: Boolean,
                    compressions: Seq[CompressionInternal])

  case class Offset(start: Int, size: Int) extends OffsetBase

  case class State(var _bytes: Slice[Byte],
                   headerSize: Int,
                   hasPrefixCompression: Boolean,
                   enableAccessPositionIndex: Boolean,
                   compressions: Seq[CompressionInternal]) {
    def bytes = _bytes

    def bytes_=(bytes: Slice[Byte]) =
      this._bytes = bytes
  }

  val hasCompressionHeaderSize = {
    val size = Block.headerSize(true)
    Bytes.sizeOf(size) +
      ByteSizeOf.boolean + //enablePositionIndex
      ByteSizeOf.boolean + //hasPrefixCompression
      size
  }

  val noCompressionHeaderSize = {
    val size = Block.headerSize(false)
    Bytes.sizeOf(size) +
      ByteSizeOf.boolean + //enablePositionIndex
      ByteSizeOf.boolean + //hasPrefixCompression
      size
  }

  def headerSize(hasCompression: Boolean): Int =
    if (hasCompression)
      hasCompressionHeaderSize
    else
      noCompressionHeaderSize

  def init(keyValues: Iterable[KeyValue.WriteOnly]): SortedIndex.State = {
    val bytes = Slice.create[Byte](keyValues.last.stats.segmentSortedIndexSize)
    val headSize = headerSize(keyValues.last.sortedIndexConfig.compressions.nonEmpty)
    bytes moveWritePosition headSize
    State(
      _bytes = bytes,
      headerSize = headSize,
      enableAccessPositionIndex = keyValues.last.sortedIndexConfig.enableAccessPositionIndex,
      hasPrefixCompression = keyValues.last.stats.hasPrefixCompression,
      compressions = keyValues.last.sortedIndexConfig.compressions
    )
  }

  def close(state: State): IO[State] =
    Block.create(
      headerSize = state.headerSize,
      bytes = state.bytes,
      compressions = state.compressions,
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

  def write(keyValue: KeyValue.WriteOnly, state: SortedIndex.State) =
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

  def read(offset: SortedIndex.Offset,
           segmentReader: BlockReader[SegmentBlock]): IO[SortedIndex] =
    Block.readHeader(
      offset = offset,
      reader = segmentReader
    ) flatMap {
      header =>
        for {
          enableAccessPositionIndex <- header.headerReader.readBoolean()
          hasPrefixCompression <- header.headerReader.readBoolean()
        } yield {
          SortedIndex(
            offset = offset,
            enableAccessPositionIndex = enableAccessPositionIndex,
            hasPrefixCompression = hasPrefixCompression,
            headerSize = header.headerSize,
            compressionInfo = header.compressionInfo
          )
        }
    }

  private def readNextKeyValue(previous: Persistent,
                               indexReader: BlockReader[SortedIndex],
                               valueReader: Option[BlockReader[Values]]): IO[Persistent] =
    readNextKeyValue(
      indexEntrySizeMayBe = Some(previous.nextIndexSize),
      indexReader = indexReader moveTo previous.nextIndexOffset,
      valueReader = valueReader,
      previous = Some(previous)
    )

  private def readNextKeyValue(fromPosition: Int,
                               indexReader: BlockReader[SortedIndex],
                               valueReader: Option[BlockReader[Values]]): IO[Persistent] =
    readNextKeyValue(
      indexEntrySizeMayBe = None,
      indexReader = indexReader moveTo fromPosition,
      valueReader = valueReader,
      previous = None
    )

  //Pre-requisite: The position of the index on the reader should be set.
  private def readNextKeyValue(indexEntrySizeMayBe: Option[Int],
                               indexReader: BlockReader[SortedIndex],
                               valueReader: Option[BlockReader[Values]],
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

      EntryReader.read(
        //take only the bytes required for this in entry and submit it for parsing/reading.
        indexReader = sortedIndexReader,
        valueReader = valueReader,
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
              sortedIndexReader: BlockReader[SortedIndex],
              valuesReader: Option[BlockReader[Values]],
              addTo: Option[Slice[KeyValue.ReadOnly]] = None): IO[Slice[KeyValue.ReadOnly]] =
    try {
      sortedIndexReader moveTo 0
      val readSortedIndexReader = sortedIndexReader.readFullBlockAndGetReader().get

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
            valueReader = valuesReader,
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
             indexReader: BlockReader[SortedIndex],
             valuesReader: Option[BlockReader[Values]])(implicit order: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] =
    search(
      matcher = KeyMatcher.Get(key),
      startFrom = startFrom,
      indexReader = indexReader,
      valuesReader = valuesReader
    )

  def searchHigher(key: Slice[Byte],
                   startFrom: Option[Persistent],
                   sortedIndexReader: BlockReader[SortedIndex],
                   valuesReader: Option[BlockReader[Values]])(implicit order: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] =
    search(
      matcher = KeyMatcher.Higher(key),
      startFrom = startFrom,
      indexReader = sortedIndexReader,
      valuesReader = valuesReader
    )

  def searchHigherSeekOne(key: Slice[Byte],
                          startFrom: Option[Persistent],
                          indexReader: BlockReader[SortedIndex],
                          valuesReader: Option[BlockReader[Values]])(implicit order: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] =
    search(
      matcher = KeyMatcher.Higher.MatchOnly(key),
      startFrom = startFrom,
      indexReader = indexReader,
      valuesReader = valuesReader
    )

  def searchLower(key: Slice[Byte],
                  startFrom: Option[Persistent],
                  indexReader: BlockReader[SortedIndex],
                  valuesReader: Option[BlockReader[Values]])(implicit order: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] =
    search(
      matcher = KeyMatcher.Lower(key),
      startFrom = startFrom,
      indexReader = indexReader,
      valuesReader = valuesReader
    )

  private def search(matcher: KeyMatcher,
                     startFrom: Option[Persistent],
                     indexReader: BlockReader[SortedIndex],
                     valuesReader: Option[BlockReader[Values]]): IO[Option[Persistent]] =
    startFrom match {
      case Some(startFrom) =>
        matchOrNextAndPersistent(
          previous = startFrom,
          next = None,
          matcher = matcher,
          indexReader = indexReader,
          valueReader = valuesReader
        )

      //No start from. Get the first index entry from the File and start from there.
      case None =>
        readNextKeyValue(
          fromPosition = 0,
          indexReader = indexReader,
          valueReader = valuesReader
        ) flatMap {
          keyValue =>
            matchOrNextAndPersistent(
              previous = keyValue,
              next = None,
              matcher = matcher,
              indexReader = indexReader,
              valueReader = valuesReader
            )
        }
    }

  def findAndMatchOrNextPersistent(matcher: KeyMatcher,
                                   fromOffset: Int,
                                   indexReader: BlockReader[SortedIndex],
                                   valueReader: Option[BlockReader[Values]]): IO[Option[Persistent]] =
    readNextKeyValue(
      fromPosition = fromOffset,
      indexReader = indexReader,
      valueReader = valueReader
    ) flatMap {
      persistent =>
        matchOrNextAndPersistent(
          previous = persistent,
          next = None,
          matcher = matcher,
          indexReader = indexReader,
          valueReader = valueReader
        )
    }

  def findAndMatchOrNextMatch(matcher: KeyMatcher,
                              fromOffset: Int,
                              sortedIndex: BlockReader[SortedIndex],
                              values: Option[BlockReader[Values]]): IO[KeyMatcher.Result] =
    readNextKeyValue(
      fromPosition = fromOffset,
      indexReader = sortedIndex,
      valueReader = values
    ) flatMap {
      persistent =>
        matchOrNext(
          previous = persistent,
          next = None,
          matcher = matcher,
          indexReader = sortedIndex,
          valueReader = values
        )
    }

  @tailrec
  def matchOrNext(previous: Persistent,
                  next: Option[Persistent],
                  matcher: KeyMatcher,
                  indexReader: BlockReader[SortedIndex],
                  valueReader: Option[BlockReader[Values]]): IO[KeyMatcher.Result.Complete] =
    matcher(
      previous = previous,
      next = next,
      hasMore = hasMore(next getOrElse previous)
    ) match {
      case KeyMatcher.Result.BehindFetchNext =>
        val readFrom = next getOrElse previous
        readNextKeyValue(
          previous = readFrom,
          indexReader = indexReader,
          valueReader = valueReader
        ) match {
          case IO.Success(nextNextKeyValue) =>
            matchOrNext(
              previous = readFrom,
              next = Some(nextNextKeyValue),
              matcher = matcher,
              indexReader = indexReader,
              valueReader = valueReader
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
                               indexReader: BlockReader[SortedIndex],
                               valueReader: Option[BlockReader[Values]]): IO[Option[Persistent]] =
    matchOrNext(
      previous = previous,
      next = next,
      matcher = matcher,
      indexReader = indexReader,
      valueReader = valueReader
    ) match {
      case IO.Success(KeyMatcher.Result.Matched(_, keyValue, _)) =>
        IO.Success(Some(keyValue))

      case IO.Success(KeyMatcher.Result.AheadOrNoneOrEnd | KeyMatcher.Result.BehindStopped) =>
        IO.none

      case IO.Failure(error) =>
        IO.Failure(error)
    }

  private def hasMore(keyValue: Persistent) =
    keyValue.nextIndexSize > 0
}

case class SortedIndex(offset: SortedIndex.Offset,
                       enableAccessPositionIndex: Boolean,
                       hasPrefixCompression: Boolean,
                       headerSize: Int,
                       compressionInfo: Option[Block.CompressionInfo]) extends Block {

  def createBlockReader(segmentReader: BlockReader[SegmentBlock]): BlockReader[SortedIndex] =
    BlockReader(
      reader = segmentReader,
      block = this
    )

  override def updateOffset(start: Int, size: Int): Block =
    copy(offset = SortedIndex.Offset(start = start, size = size))
}
