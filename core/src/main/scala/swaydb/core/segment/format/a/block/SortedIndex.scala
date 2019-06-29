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
import swaydb.core.segment.format.a.{KeyMatcher, MatchResult, OffsetBase}
import swaydb.core.util.Bytes
import swaydb.data.IO
import swaydb.data.IO._
import swaydb.data.config.SortedIndex
import swaydb.data.slice.{Reader, Slice}

import scala.annotation.tailrec

private[core] object SortedIndex {

  object Config {
    val disabled =
      Config(
        cacheOnRead = false,
        prefixCompressionResetCount = 0,
        hasCompression = false
      )

    def apply(config: swaydb.data.config.SortedIndex): Config =
      config match {
        case config: swaydb.data.config.SortedIndex.Enable =>
          apply(config)
      }

    def apply(config: swaydb.data.config.SortedIndex.Enable): Config =
      Config(
        cacheOnRead = config.cacheOnRead,
        prefixCompressionResetCount = config.prefixCompression.toOption.flatMap(_.resetCount).getOrElse(0),
        hasCompression = config.compression.nonEmpty
      )

    def enablePrefixCompression(config: Config,
                                previous: Option[KeyValue.WriteOnly]): Boolean =
      config.prefixCompressionResetCount > 0 &&
        previous.exists {
          previous =>
            (previous.stats.chainPosition + 1) % config.prefixCompressionResetCount == 0
        }
  }

  case class Config(cacheOnRead: Boolean,
                    prefixCompressionResetCount: Int,
                    hasCompression: Boolean)

  case class Offset(start: Int, size: Int) extends OffsetBase

  case class State(var _bytes: Slice[Byte],
                   headerSize: Int,
                   compressions: Seq[CompressionInternal]) {
    def bytes = _bytes

    def bytes_=(bytes: Slice[Byte]) =
      this._bytes = bytes
  }

  def headerSize(hasCompression: Boolean): Int = {
    val size = Block.headerSize(hasCompression)
    Bytes.sizeOf(size) +
      size
  }

  def init(keyValues: Iterable[KeyValue.WriteOnly],
           compressions: Seq[CompressionInternal]): SortedIndex.State = {
    val bytes = Slice.create[Byte](keyValues.last.stats.segmentSortedIndexSize)
    val headSize = headerSize(compressions.nonEmpty)
    bytes moveWritePosition headSize
    State(
      _bytes = bytes,
      headerSize = headSize,
      compressions = compressions
    )
  }

  def close(state: State): IO[Unit] =
    Block.create(
      headerSize = state.headerSize,
      bytes = state.bytes,
      compressions = state.compressions
    ) flatMap {
      compressedOrUncompressedBytes =>
        state.bytes = compressedOrUncompressedBytes
        if (state.bytes.currentWritePosition > state.headerSize)
          IO.Failure(IO.Error.Fatal(s"Calculated header size was incorrect. Expected: ${state.headerSize}. Used: ${state.bytes.currentWritePosition - 1}"))
        else
          IO.unit
    }

  def read(offset: SortedIndex.Offset,
           segmentReader: Reader): IO[SortedIndex] =
    Block.readHeader(
      offset = offset,
      segmentReader = segmentReader
    ) map {
      header =>
        SortedIndex(
          blockOffset = offset,
          headerSize = header.headerSize,
          compressionInfo = header.compressionInfo
        )
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
      val bytesToRead = indexSize + 5

      //read all bytes for this index entry plus the next 5 bytes to fetch next index entry's size.
      val indexEntryBytesAndNextIndexEntrySize = (indexReader read bytesToRead).get

      val extraBytesRead = indexEntryBytesAndNextIndexEntrySize.size - indexSize

      //The above fetches another 5 bytes (unsigned int) along with previous index entry.
      //These 5 bytes contains the next index's size. Here the next key-values indexSize and indexOffset are read.
      val (nextIndexSize, nextIndexOffset) =
      if (extraBytesRead == 0) {
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

      EntryReader.read(
        //take only the bytes required for this in entry and submit it for parsing/reading.
        indexReader = Reader(indexEntryBytesAndNextIndexEntrySize.take(indexSize)),
        valueReader = valueReader,
        indexOffset = positionBeforeRead,
        nextIndexOffset = nextIndexOffset,
        nextIndexSize = nextIndexSize,
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
                  message = s"Corrupted Segment: Failed to read index entry at reader position ${indexReader.getPosition}$atPosition}",
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

  def find(matcher: KeyMatcher,
           startFrom: Option[Persistent],
           indexReader: BlockReader[SortedIndex],
           valuesReader: Option[BlockReader[Values]]): IO[Option[Persistent]] =
    startFrom match {
      case Some(startFrom) =>
        //if startFrom is the last index entry, return None.
        if (startFrom.nextIndexSize == 0)
          IO.none
        else
          readNextKeyValue(
            previous = startFrom,
            indexReader = indexReader,
            valueReader = valuesReader
          ) flatMap {
            keyValue =>
              matchOrNextAndGet(
                previous = startFrom,
                next = Some(keyValue),
                matcher = matcher,
                indexReader = indexReader,
                valueReader = valuesReader
              )
          }

      //No start from. Get the first index entry from the File and start from there.
      case None =>
        readNextKeyValue(
          fromPosition = 0,
          indexReader = indexReader,
          valueReader = valuesReader
        ) flatMap {
          keyValue =>
            matchOrNextAndGet(
              previous = keyValue,
              next = None,
              matcher = matcher,
              indexReader = indexReader,
              valueReader = valuesReader
            )
        }
    }

  def findAndMatchOrNext(matcher: KeyMatcher,
                         fromOffset: Int,
                         indexReader: BlockReader[SortedIndex],
                         valueReader: Option[BlockReader[Values]]): IO[Option[Persistent]] =
    readNextKeyValue(
      fromPosition = fromOffset,
      indexReader = indexReader,
      valueReader = valueReader
    ) flatMap {
      persistent =>
        matchOrNextAndGet(
          previous = persistent,
          next = None,
          matcher = matcher,
          indexReader = indexReader,
          valueReader = valueReader
        )
    }

  def findAndMatch(matcher: KeyMatcher,
                   fromOffset: Int,
                   sortedIndex: BlockReader[SortedIndex],
                   values: Option[BlockReader[Values]]): IO[MatchResult] =
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
                  valueReader: Option[BlockReader[Values]]): IO[MatchResult] =
    matcher(
      previous = previous,
      next = next,
      hasMore = hasMore(next getOrElse previous)
    ) match {
      case MatchResult.Behind =>
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

      case result @ (MatchResult.Matched(_) | MatchResult.BehindStopped | MatchResult.AheadOrEnd) =>
        result.asIO
    }

  @tailrec
  def matchOrNextAndGet(previous: Persistent,
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
      case IO.Success(MatchResult.Behind) =>
        matchOrNextAndGet(
          previous = previous,
          next = next,
          matcher = matcher,
          indexReader = indexReader,
          valueReader = valueReader
        )

      case IO.Success(MatchResult.Matched(keyValue)) =>
        IO.Success(Some(keyValue))

      case IO.Success(MatchResult.AheadOrEnd | MatchResult.BehindStopped) =>
        IO.none

      case IO.Failure(error) =>
        IO.Failure(error)
    }

  private def hasMore(keyValue: Persistent) =
    keyValue.nextIndexSize > 0
}

case class SortedIndex(blockOffset: SortedIndex.Offset,
                       headerSize: Int,
                       compressionInfo: Option[Block.CompressionInfo]) extends Block {

  override def createBlockReader(bytes: Slice[Byte]): BlockReader[SortedIndex] =
    createBlockReader(Reader(bytes))

  def createBlockReader(segmentReader: Reader): BlockReader[SortedIndex] =
    BlockReader(
      segmentReader = segmentReader,
      block = this
    )

  override def updateOffset(start: Int, size: Int): Block =
    copy(blockOffset = SortedIndex.Offset(start = start, size = size))
}
