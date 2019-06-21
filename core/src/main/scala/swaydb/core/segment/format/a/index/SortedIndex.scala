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

package swaydb.core.segment.format.a.index

import swaydb.core.data.{KeyValue, Persistent}
import swaydb.core.io.reader.Reader
import swaydb.core.segment.SegmentException.SegmentCorruptionException
import swaydb.core.segment.format.a.entry.reader.EntryReader
import swaydb.core.segment.format.a.{KeyMatcher, MatchResult, OffsetBase}
import swaydb.core.util.Bytes
import swaydb.data.IO
import swaydb.data.IO._
import swaydb.data.order.KeyOrder
import swaydb.data.slice.{Reader, Slice}

import scala.annotation.tailrec

private[core] object SortedIndex {

  case class Offset(start: Int, size: Int) extends OffsetBase

  def readNextKeyValue(previous: Persistent,
                       startOffset: Int,
                       endOffset: Int,
                       indexReader: Reader,
                       valueReader: Reader)(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Persistent] = {
    indexReader moveTo previous.nextIndexOffset
    readNextKeyValue(
      indexEntrySizeMayBe = Some(previous.nextIndexSize),
      adjustNextIndexOffsetBy = 0,
      startIndexOffset = startOffset,
      endIndexOffset = endOffset,
      indexReader = indexReader,
      valueReader = valueReader,
      previous = Some(previous)
    )
  }

  private def readNextKeyValue(fromPosition: Int,
                               startIndexOffset: Int,
                               endIndexOffset: Int,
                               indexReader: Reader,
                               valueReader: Reader)(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Persistent] = {
    indexReader moveTo fromPosition
    readNextKeyValue(
      indexEntrySizeMayBe = None,
      adjustNextIndexOffsetBy = 0,
      startIndexOffset = startIndexOffset,
      endIndexOffset = endIndexOffset,
      indexReader = indexReader,
      valueReader = valueReader,
      previous = None
    )
  }

  //Pre-requisite: The position of the index on the reader should be set.
  private def readNextKeyValue(indexEntrySizeMayBe: Option[Int],
                               adjustNextIndexOffsetBy: Int, //the reader could be a sub slice. This is used to adjust next indexOffset size
                               startIndexOffset: Int,
                               endIndexOffset: Int,
                               indexReader: Reader,
                               valueReader: Reader,
                               previous: Option[Persistent])(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Persistent] =
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

      val remainingIndexBytesTillEnd = endIndexOffset - (indexReader.getPosition - 1 + indexSize)
      //5 extra bytes are read for each entry to fetch the next index's size.
      val extraTailBytesToRead = remainingIndexBytesTillEnd min 5
      val bytesToRead = indexSize + extraTailBytesToRead

      //read all bytes for this index entry plus the next 5 bytes to fetch next index entry's size.
      val indexEntryBytesAndNextIndexEntrySize = (indexReader read bytesToRead).get

      //take only the bytes required for this in entry and submit it for parsing/reading.
      val indexEntryReader = Reader(indexEntryBytesAndNextIndexEntrySize.take(indexSize))

      //The above fetches another 5 bytes (unsigned int) along with previous index entry.
      //These 5 bytes contains the next index's size. Here the next key-values indexSize and indexOffset are read.
      val (nextIndexSize, nextIndexOffset) =
      if (extraTailBytesToRead > 0) { //if extra tail byte were read this mean that this index has a next key-value.
        //next indexEntrySize is only read if it's required.
        val nextIndexEntrySize = Reader(indexEntryBytesAndNextIndexEntrySize.drop(indexSize))
        (nextIndexEntrySize.readIntUnsigned().get, indexReader.getPosition - extraTailBytesToRead + adjustNextIndexOffsetBy)
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

  def readAll(offset: SortedIndex.Offset,
              keyValueCount: Int,
              reader: Reader,
              addTo: Option[Slice[KeyValue.ReadOnly]] = None)(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Slice[KeyValue.ReadOnly]] =
    try {
      //since this is a index slice of the full Segment, adjustments for nextIndexOffset is required.
      val adjustNextIndexOffsetBy = offset.start
      //read full index in one disk seek and Slice it to KeyValue chunks.
      val sortedIndexReader = reader moveTo offset.start read offset.size map (Reader(_)) get
      val endIndexOffset: Int = sortedIndexReader.size.get.toInt - 1

      val entries = addTo getOrElse Slice.create[Persistent](keyValueCount)
      (1 to keyValueCount).foldLeftIO(Option.empty[Persistent]) {
        case (previousMayBe, _) =>
          val nextIndexSize =
            previousMayBe map {
              previous =>
                //If previous is known, keep reading same reader
                // and set the next position of the reader to be of the next index's offset.
                sortedIndexReader moveTo (previous.nextIndexOffset - adjustNextIndexOffsetBy)
                previous.nextIndexSize
            }

          readNextKeyValue(
            indexEntrySizeMayBe = nextIndexSize,
            adjustNextIndexOffsetBy = adjustNextIndexOffsetBy,
            startIndexOffset = previousMayBe.map(_.nextIndexOffset).getOrElse(offset.start),
            endIndexOffset = endIndexOffset,
            indexReader = sortedIndexReader,
            valueReader = reader.copy(),
            //user entries.lastOption instead of previousMayBe because, addTo might already be pre-populated and the
            //last entry would of bethe.
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

  def readBytes(fromOffset: Int, length: Int, reader: Reader): IO[Option[Slice[Byte]]] =
    try {
      if (length == 0)
        IO.none
      else
        (reader.copy() moveTo fromOffset read length).map(Some(_))
    } catch {
      case exception: Exception =>
        exception match {
          case _: ArrayIndexOutOfBoundsException | _: IndexOutOfBoundsException | _: IllegalArgumentException | _: NegativeArraySizeException =>
            IO.Failure(
              IO.Error.Fatal(
                SegmentCorruptionException(
                  message = s"Corrupted Segment: Failed to get bytes of length $length from offset $fromOffset",
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
           reader: Reader,
           offset: SortedIndex.Offset)(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] =
    startFrom match {
      case Some(startFrom) =>
        //if startFrom is the last index entry, return None.
        if (startFrom.nextIndexSize == 0)
          IO.none
        else
          readNextKeyValue(
            previous = startFrom,
            startOffset = offset.start,
            endOffset = offset.end,
            indexReader = reader,
            valueReader = reader
          ) flatMap {
            keyValue =>
              matchOrNext(
                previous = startFrom,
                next = Some(keyValue),
                matcher = matcher,
                reader = reader,
                offset = offset
              )
          }

      //No start from. Get the first index entry from the File and start from there.
      case None =>
        readNextKeyValue(
          fromPosition = offset.start,
          startIndexOffset = offset.start,
          endIndexOffset = offset.end,
          indexReader = reader,
          valueReader = reader
        ) flatMap {
          keyValue =>
            matchOrNext(
              previous = keyValue,
              next = None,
              matcher = matcher,
              reader = reader,
              offset = offset
            )
        }
    }

  def findAndMatchOrNext(matcher: KeyMatcher,
                         fromOffset: Int,
                         reader: Reader,
                         offset: SortedIndex.Offset)(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] =
    readNextKeyValue(
      fromPosition = fromOffset,
      startIndexOffset = offset.start,
      endIndexOffset = offset.end,
      indexReader = reader,
      valueReader = reader
    ) flatMap {
      persistent =>
        matchOrNext(
          previous = persistent,
          next = None,
          matcher = matcher,
          reader = reader,
          offset = offset
        )
    }

  def findAndMatch(matcher: KeyMatcher,
                   fromOffset: Int,
                   reader: Reader,
                   offset: SortedIndex.Offset)(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[MatchResult] =
    readNextKeyValue(
      fromPosition = fromOffset,
      startIndexOffset = offset.start,
      endIndexOffset = offset.end,
      indexReader = reader,
      valueReader = reader
    ) map {
      persistent =>
        matcher(
          previous = persistent,
          next = None,
          hasMore = hasMore(persistent, offset)
        )
    }

  @tailrec
  def matchOrNext(previous: Persistent,
                  next: Option[Persistent],
                  matcher: KeyMatcher,
                  reader: Reader,
                  offset: SortedIndex.Offset)(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] =
    matcher(
      previous = previous,
      next = next,
      hasMore = hasMore(next getOrElse previous, offset)
    ) match {
      case MatchResult.Next =>
        val readFrom = next getOrElse previous
        SortedIndex.readNextKeyValue(
          previous = readFrom,
          startOffset = offset.start,
          endOffset = offset.end,
          indexReader = reader,
          valueReader = reader
        ) match {
          case IO.Success(nextNextKeyValue) =>
            matchOrNext(
              previous = readFrom,
              next = Some(nextNextKeyValue),
              matcher = matcher,
              reader = reader,
              offset = offset
            )

          case IO.Failure(exception) =>
            IO.Failure(exception)
        }

      case MatchResult.Matched(keyValue) =>
        IO.Success(Some(keyValue))

      case MatchResult.Stop =>
        IO.none
    }

  private def hasMore(keyValue: Persistent, offset: SortedIndex.Offset) =
    keyValue.nextIndexOffset >= 0 && keyValue.nextIndexOffset < offset.end
}
