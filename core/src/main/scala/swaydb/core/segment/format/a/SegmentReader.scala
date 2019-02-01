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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.segment.format.a

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.data.{KeyValue, Persistent}
import swaydb.core.io.reader.Reader
import swaydb.core.segment.SegmentException.SegmentCorruptionException
import swaydb.core.segment.format.a.entry.reader.EntryReader
import swaydb.core.util.BloomFilterUtil._
import swaydb.core.util.IOUtil._
import swaydb.core.util.{Bytes, CRC32, IOUtil}
import swaydb.data.slice.Slice._
import swaydb.data.slice.{Reader, Slice}
import swaydb.data.util.ByteSizeOf
import scala.annotation.tailrec
import swaydb.data.io.IO
import swaydb.data.order.KeyOrder

/**
  * All public APIs are wrapped around a try catch block because eager fetches on IO's results (.get).
  * Eventually need to re-factor this code to use for comprehension.
  *
  * Leaving as it is now because it's easier to read.
  */
private[core] object SegmentReader extends LazyLogging {

  private def readNextKeyValue(previous: Persistent,
                               startIndexOffset: Int,
                               endIndexOffset: Int,
                               indexReader: Reader,
                               valueReader: Reader)(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Persistent] = {
    indexReader moveTo previous.nextIndexOffset
    readNextKeyValue(
      indexEntrySizeMayBe = Some(previous.nextIndexSize),
      adjustNextIndexOffsetBy = 0,
      startIndexOffset = startIndexOffset,
      endIndexOffset = endIndexOffset,
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
              SegmentCorruptionException(
                s"Corrupted Segment: Failed to read index entry at reader position ${indexReader.getPosition}$atPosition}",
                exception
              )
            )

          case ex: Exception =>
            IO.Failure(ex)
        }
    }

  def readAll(footer: SegmentFooter,
              reader: Reader,
              addTo: Option[Slice[KeyValue.ReadOnly]] = None)(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Slice[KeyValue.ReadOnly]] =
    try {
      //since this is a index slice of the full Segment, adjustments for nextIndexOffset is required.
      val adjustNextIndexOffsetBy = footer.startIndexOffset
      //read full index in one disk seek and Slice it to KeyValue chunks.
      val indexOnlyReader = Reader((reader moveTo footer.startIndexOffset read (footer.endIndexOffset - footer.startIndexOffset + 1)).get)
      val endIndexOffset: Int = indexOnlyReader.size.get.toInt - 1

      val entries = addTo getOrElse Slice.create[Persistent](footer.keyValueCount)
      (1 to footer.keyValueCount).tryFoldLeft(Option.empty[Persistent]) {
        case (previousMayBe, _) =>
          val nextIndexSize =
            previousMayBe map {
              previous =>
                //If previous is known, keep reading same reader
                // and set the next position of the reader to be of the next index's offset.
                indexOnlyReader moveTo (previous.nextIndexOffset - adjustNextIndexOffsetBy)
                previous.nextIndexSize
            }

          readNextKeyValue(
            indexEntrySizeMayBe = nextIndexSize,
            adjustNextIndexOffsetBy = adjustNextIndexOffsetBy,
            startIndexOffset = previousMayBe.map(_.nextIndexOffset).getOrElse(footer.startIndexOffset),
            endIndexOffset = endIndexOffset,
            indexReader = indexOnlyReader,
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
            IO.Failure(SegmentCorruptionException(s"Corrupted Segment: Failed to read index bytes", exception))

          case ex: Exception =>
            IO.Failure(ex)
        }
    }

  def readBytes(fromOffset: Int, length: Int, reader: Reader): IO[Option[Slice[Byte]]] =
    try {
      if (length == 0)
        IOUtil.successNone
      else
        (reader.copy() moveTo fromOffset read length).map(Some(_))
    } catch {
      case exception: Exception =>
        exception match {
          case _: ArrayIndexOutOfBoundsException | _: IndexOutOfBoundsException | _: IllegalArgumentException | _: NegativeArraySizeException =>
            IO.Failure(
              SegmentCorruptionException(
                s"Corrupted Segment: Failed to get bytes of length $length from offset $fromOffset",
                exception
              )
            )

          case ex: Exception =>
            IO.Failure(ex)
        }
    }

  def readFooter(reader: Reader): IO[SegmentFooter] =
    try {
      val fileSize = reader.size.get
      val footerSize = reader.moveTo(fileSize - ByteSizeOf.int).readInt().get
      val footerBytes = reader.moveTo(fileSize - footerSize).read(footerSize - ByteSizeOf.int)
      val footerReader = Reader(footerBytes.get)
      val formatId = footerReader.readIntUnsigned().get
      assert(formatId == SegmentWriter.formatId, s"Invalid Segment formatId: $formatId. Expected: ${SegmentWriter.formatId}")
      val hasRange = footerReader.readBoolean().get
      val hasPut = footerReader.readBoolean().get
      val indexStartOffset = footerReader.readIntUnsigned().get
      val expectedCRC = footerReader.readLong().get
      val keyValueCount = footerReader.readIntUnsigned().get
      val bloomFilterItemsCount = footerReader.readIntUnsigned().get
      val bloomFilterSize = footerReader.readIntUnsigned().get
      val bloomFilterSlice =
        if (bloomFilterSize == 0)
          None
        else
          Some(footerReader.read(bloomFilterSize).get)

      val crcBytes = reader.moveTo(indexStartOffset).read(SegmentWriter.crcBytes).get
      val crc = CRC32.forBytes(crcBytes)
      if (expectedCRC != crc) {
        IO.Failure(SegmentCorruptionException(s"Corrupted Segment: CRC Check failed. $expectedCRC != $crc", new Exception("CRC check failed.")))
      } else {
        val indexEndOffset = fileSize.toInt - footerSize - 1
        IO.Success(
          SegmentFooter(
            crc = expectedCRC,
            startIndexOffset = indexStartOffset,
            endIndexOffset = indexEndOffset,
            keyValueCount = keyValueCount,
            hasRange = hasRange,
            hasPut = hasPut,
            bloomFilterItemsCount = bloomFilterItemsCount,
            bloomFilter = bloomFilterSlice.map(_.toBloomFilter)
          )
        )
      }
    } catch {
      case exception: Exception =>
        exception match {
          case _: ArrayIndexOutOfBoundsException | _: IndexOutOfBoundsException | _: IllegalArgumentException | _: NegativeArraySizeException =>
            IO.Failure(SegmentCorruptionException("Corrupted Segment: Failed to read footer bytes", exception))

          case ex: Exception =>
            IO.Failure(ex)
        }
    }

  def find(matcher: KeyMatcher,
           startFrom: Option[Persistent],
           reader: Reader)(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] =
    readFooter(reader) flatMap (find(matcher, startFrom, reader, _))

  def find(matcher: KeyMatcher,
           startFrom: Option[Persistent],
           reader: Reader,
           footer: SegmentFooter)(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] =
    try {
      startFrom match {
        case Some(startFrom) =>
          //if startFrom is the last index entry, return None.
          if (startFrom.nextIndexSize == 0)
            IOUtil.successNone
          else
            readNextKeyValue(
              previous = startFrom,
              startIndexOffset = footer.startIndexOffset,
              endIndexOffset = footer.endIndexOffset,
              indexReader = reader,
              valueReader = reader
            ) flatMap {
              keyValue =>
                find(startFrom, Some(keyValue), matcher, reader, footer)
            }

        //No start from. Get the first index entry from the File and start from there.
        case None =>
          readNextKeyValue(
            fromPosition = footer.startIndexOffset,
            startIndexOffset = footer.startIndexOffset,
            endIndexOffset = footer.endIndexOffset,
            indexReader = reader,
            valueReader = reader
          ) flatMap {
            keyValue =>
              find(keyValue, None, matcher, reader, footer)
          }
      }
    } catch {
      case ex: Exception =>
        IO.Failure(ex)
    }

  @tailrec
  private def find(previous: Persistent,
                   next: Option[Persistent],
                   matcher: KeyMatcher,
                   reader: Reader,
                   footer: SegmentFooter)(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] =
    matcher(previous, next, hasMore = hasMore(next getOrElse previous, footer)) match {
      case MatchResult.Next =>
        val readFrom = next getOrElse previous
        readNextKeyValue(
          previous = readFrom,
          startIndexOffset = footer.startIndexOffset,
          endIndexOffset = footer.endIndexOffset,
          indexReader = reader,
          valueReader = reader
        ) match {
          case IO.Success(nextNextKeyValue) =>
            find(readFrom, Some(nextNextKeyValue), matcher, reader, footer)

          case IO.Failure(exception) =>
            IO.Failure(exception)
        }

      case MatchResult.Matched(keyValue) =>
        IO.Success(Some(keyValue))

      case MatchResult.Stop =>
        IOUtil.successNone

    }

  private def hasMore(keyValue: Persistent, footer: SegmentFooter) =
    keyValue.nextIndexOffset >= 0 && keyValue.nextIndexOffset < footer.endIndexOffset

}
