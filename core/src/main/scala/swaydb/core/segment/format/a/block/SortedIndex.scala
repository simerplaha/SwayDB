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
import swaydb.data.slice.{Reader, Slice}

import scala.annotation.tailrec

private[core] object SortedIndex {

  case class Offset(start: Int, size: Int) extends OffsetBase

  case class State(var _bytes: Slice[Byte],
                   headerSize: Int,
                   compressions: Seq[CompressionInternal]) {
    def bytes = _bytes

    def bytes_=(bytes: Slice[Byte]) =
      this._bytes = bytes
  }

  val headerSize =
    Block.headerSize

  def init(keyValues: Iterable[KeyValue.WriteOnly],
           compressions: Seq[CompressionInternal]): SortedIndex.State = {
    val bytes = Slice.create[Byte](keyValues.last.stats.segmentSortedIndexSize)
    bytes moveWritePosition SortedIndex.headerSize
    State(
      _bytes = bytes,
      headerSize = SortedIndex.headerSize,
      compressions = compressions
    )
  }

  def close(state: State): IO[Unit] =
    Block.compress(
      headerSize = state.headerSize,
      bytes = state.bytes,
      compressions = state.compressions
    ) flatMap {
      compressedOrUncompressedBytes =>
        IO {
          state.bytes = compressedOrUncompressedBytes
          if (state.bytes.currentWritePosition > state.headerSize)
            throw new Exception(s"Calculated header size was incorrect. Expected: ${state.headerSize}. Used: ${state.bytes.currentWritePosition - 1}")
        }
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
                               valueReader: Option[BlockReader[Values]]): IO[Persistent] = {
    indexReader moveTo previous.nextIndexOffset
    readNextKeyValue(
      indexEntrySizeMayBe = Some(previous.nextIndexSize),
      indexReader = indexReader,
      valueReader = valueReader,
      previous = Some(previous)
    )
  }

  private def readNextKeyValue(fromPosition: Int,
                               indexReader: BlockReader[SortedIndex],
                               valueReader: Option[BlockReader[Values]]): IO[Persistent] = {
    indexReader moveTo fromPosition
    readNextKeyValue(
      indexEntrySizeMayBe = None,
      indexReader = indexReader,
      valueReader = valueReader,
      previous = None
    )
  }

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

      //take only the bytes required for this in entry and submit it for parsing/reading.
      val indexEntryReader = Reader(indexEntryBytesAndNextIndexEntrySize.take(indexSize))

      //The above fetches another 5 bytes (unsigned int) along with previous index entry.
      //These 5 bytes contains the next index's size. Here the next key-values indexSize and indexOffset are read.
      val (nextIndexSize, nextIndexOffset) =
      if (indexReader.hasMore.get) { //if extra tail byte were read this mean that this index has a next key-value.
        //next indexEntrySize is only read if it's required.
        val nextIndexEntrySize = Reader(indexEntryBytesAndNextIndexEntrySize.drop(indexSize))
        (nextIndexEntrySize.readIntUnsigned().get, indexReader.getPosition - 5)
      } else {
        //no next key-value, next size is 0 and set offset to -1.
        (0, -1)
      }

      EntryReader.read(
        indexReader = indexEntryReader,
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
              valueReader: Option[BlockReader[Values]],
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
            valueReader = valueReader,
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
           valueReader: Option[BlockReader[Values]]): IO[Option[Persistent]] =
  //    startFrom match {
  //      case Some(startFrom) =>
  //        //if startFrom is the last index entry, return None.
  //        if (startFrom.nextIndexSize == 0)
  //          IO.none
  //        else
  //          readNextKeyValue(
  //            previous = startFrom,
  //            startOffset = index.offset.start,
  //            endOffset = index.offset.end,
  //            indexReader = segmentReader,
  //            valueReader = segmentReader
  //          ) flatMap {
  //            keyValue =>
  //              matchOrNext(
  //                previous = startFrom,
  //                next = Some(keyValue),
  //                matcher = matcher,
  //                segmentReader = segmentReader,
  //                sortedIndex = index
  //              )
  //          }
  //
  //      //No start from. Get the first index entry from the File and start from there.
  //      case None =>
  //        readNextKeyValue(
  //          fromPosition = index.offset.start,
  //          startIndexOffset = index.offset.start,
  //          endIndexOffset = index.offset.end,
  //          indexReader = segmentReader,
  //          valueReader = segmentReader
  //        ) flatMap {
  //          keyValue =>
  //            matchOrNext(
  //              previous = keyValue,
  //              next = None,
  //              matcher = matcher,
  //              segmentReader = segmentReader,
  //              sortedIndex = index
  //            )
  //        }
  //    }
    ???

  def findAndMatchOrNext(matcher: KeyMatcher,
                         fromOffset: Int,
                         indexReader: BlockReader[SortedIndex],
                         valueReader: BlockReader[Values]): IO[Option[Persistent]] =
  //    readNextKeyValue(
  //      fromPosition = fromOffset,
  //      indexReader = segmentReader,
  //      valueReader = segmentReader
  //    ) flatMap {
  //      persistent =>
  //        matchOrNext(
  //          previous = persistent,
  //          next = None,
  //          matcher = matcher,
  //          segmentReader = segmentReader,
  //          sortedIndex = index
  //        )
  //    }
    ???

  def findAndMatch(matcher: KeyMatcher,
                   fromOffset: Int,
                   sortedIndex: BlockReader[SortedIndex],
                   values: Option[BlockReader[Values]]): IO[MatchResult] =
  //    readNextKeyValue(
  //      fromPosition = fromOffset,
  //      indexReader = segmentReader,
  //      valueReader = segmentReader
  //    ) map {
  //      persistent =>
  //        matcher(
  //          previous = persistent,
  //          next = None,
  //          hasMore = hasMore(persistent, sortedIndex)
  //        )
  //    }
    ???

  //  @tailrec
  def matchOrNext(previous: Persistent,
                  next: Option[Persistent],
                  matcher: KeyMatcher,
                  indexReader: BlockReader[SortedIndex],
                  valueReader: Option[BlockReader[Values]]): IO[Option[Persistent]] =
  //    matcher(
  //      previous = previous,
  //      next = next,
  //      hasMore = hasMore(next getOrElse previous, sortedIndex.offset)
  //    ) match {
  //      case MatchResult.Next =>
  //        val readFrom = next getOrElse previous
  //        SortedIndex.readNextKeyValue(
  //          previous = readFrom,
  //          indexReader = segmentReader,
  //          valueReader = segmentReader
  //        ) match {
  //          case IO.Success(nextNextKeyValue) =>
  //            matchOrNext(
  //              previous = readFrom,
  //              next = Some(nextNextKeyValue),
  //              matcher = matcher,
  //              segmentReader = segmentReader,
  //              sortedIndex = sortedIndex
  //            )
  //
  //          case IO.Failure(exception) =>
  //            IO.Failure(exception)
  //        }
  //
  //      case MatchResult.Matched(keyValue) =>
  //        IO.Success(Some(keyValue))
  //
  //      case MatchResult.Stop =>
  //        IO.none
  //    }
    ???

  private def hasMore(keyValue: Persistent, offset: SortedIndex.Offset) =
    keyValue.nextIndexOffset >= 0 && keyValue.nextIndexOffset < offset.end
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
