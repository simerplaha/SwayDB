/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
 *
 * This file is a part of SwayDB.
 *
 * SwayDB is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 * SwayDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
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
import swaydb.core.util.{BloomFilter, Bytes, CRC32}
import swaydb.data.IO
import swaydb.data.IO._
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice._
import swaydb.data.slice.{Reader, Slice}
import swaydb.data.util.ByteSizeOf

import scala.annotation.tailrec

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

  def readAll(footer: SegmentFooter,
              reader: Reader,
              addTo: Option[Slice[KeyValue.ReadOnly]] = None)(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Slice[KeyValue.ReadOnly]] =
    try {
      //since this is a index slice of the full Segment, adjustments for nextIndexOffset is required.
      val adjustNextIndexOffsetBy = footer.sortedIndexStartOffset
      //read full index in one disk seek and Slice it to KeyValue chunks.
      val indexOnlyReader = Reader((reader moveTo footer.sortedIndexStartOffset read (footer.sortedIndexEndOffset - footer.sortedIndexStartOffset + 1)).get)
      val endIndexOffset: Int = indexOnlyReader.size.get.toInt - 1

      val entries = addTo getOrElse Slice.create[Persistent](footer.keyValueCount)
      (1 to footer.keyValueCount).foldLeftIO(Option.empty[Persistent]) {
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
            startIndexOffset = previousMayBe.map(_.nextIndexOffset).getOrElse(footer.sortedIndexStartOffset),
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

  def readHashIndexHeader(reader: Reader,
                          footer: SegmentFooter): IO[SegmentHashIndex.Header] =
    reader
      .moveTo(footer.hashIndexStartOffset)
      .read(footer.hashIndexSize)
      .flatMap {
        bytes =>
          SegmentHashIndex.readHeader(Reader(bytes))
      }

  //all these functions are wrapper with a try catch block with get only to make it easier to read.
  def readFooter(reader: Reader)(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[SegmentFooter] =
    try {
      val fileSize = reader.size.get
      val footerStartOffset = reader.moveTo(fileSize - ByteSizeOf.int).readInt().get
      val footerSize = fileSize.toInt - footerStartOffset
      val footerBytes = reader.moveTo(footerStartOffset).read(footerSize - ByteSizeOf.int)
      val footerReader = Reader(footerBytes.get)
      val formatId = footerReader.readIntUnsigned().get
      if (formatId != SegmentWriter.formatId) {
        val message = s"Invalid Segment formatId: $formatId. Expected: ${SegmentWriter.formatId}"
        return IO.Failure(IO.Error.Fatal(SegmentCorruptionException(message = message, cause = new Exception(message))))
      }
      assert(formatId == SegmentWriter.formatId, s"Invalid Segment formatId: $formatId. Expected: ${SegmentWriter.formatId}")
      val createdInLevel = footerReader.readIntUnsigned().get
      val isGrouped = footerReader.readBoolean().get
      val hasRange = footerReader.readBoolean().get
      val hasPut = footerReader.readBoolean().get
      val indexStartOffset = footerReader.readIntUnsigned().get
      val hashIndexStartOffset = footerReader.readIntUnsigned().get
      val expectedCRC = footerReader.readLong().get
      val keyValueCount = footerReader.readIntUnsigned().get
      val bloomFilterItemsCount = footerReader.readIntUnsigned().get
      val bloomFilterSize = footerReader.readInt().get
      val bloomAndRangeFilterSlice =
        if (bloomFilterSize == 0) {
          val rangeFilterByteSize = footerReader.readIntUnsigned().get
          if (rangeFilterByteSize != 1) return IO.Failure(SegmentCorruptionException(s"Range filter byte size was $rangeFilterByteSize. Expected 0.", new Exception("CRC check failed.")))
          None
        } else {
          val bloomFilterSlice = footerReader.read(bloomFilterSize).get
          val rangeFilterSize = footerReader.readIntUnsigned().get
          val rangeFilterSlice = footerReader.read(rangeFilterSize).get
          Some(bloomFilterSlice, rangeFilterSlice)
        }

      val crcBytes = reader.moveTo(indexStartOffset).read(SegmentWriter.crcBytes).get
      val crc = CRC32.forBytes(crcBytes)
      if (expectedCRC != crc) {
        IO.Failure(SegmentCorruptionException(s"Corrupted Segment: CRC Check failed. $expectedCRC != $crc", new Exception("CRC check failed.")))
      } else {
        val hashIndexEndOffset = fileSize.toInt - footerSize - 1
        val hashIndexSize = hashIndexEndOffset - hashIndexStartOffset
        val indexEndOffset = fileSize.toInt - hashIndexSize - footerSize - 2
        IO.Success(
          SegmentFooter(
            crc = expectedCRC,
            createdInLevel = createdInLevel,
            isGrouped = isGrouped,
            sortedIndexStartOffset = indexStartOffset,
            sortedIndexEndOffset = indexEndOffset,
            hashIndexStartOffset = hashIndexStartOffset,
            hashIndexEndOffset = hashIndexEndOffset,
            keyValueCount = keyValueCount,
            hasRange = hasRange,
            hasPut = hasPut,
            bloomFilterItemsCount = bloomFilterItemsCount,
            bloomFilter =
              bloomAndRangeFilterSlice map {
                case (bloomFilterSlice, rangeFilterSlice) =>
                  BloomFilter(bloomFilterSlice, rangeFilterSlice).get
              }
          )
        )
      }
    } catch {
      case exception: Exception =>
        exception match {
          case _: ArrayIndexOutOfBoundsException | _: IndexOutOfBoundsException | _: IllegalArgumentException | _: NegativeArraySizeException =>
            IO.Failure(
              IO.Error.Fatal(
                SegmentCorruptionException(
                  message = "Corrupted Segment: Failed to read footer bytes",
                  cause = exception
                )
              )
            )

          case ex: Exception =>
            IO.Failure(ex)
        }
    }

  def find(matcher: KeyMatcher.Get,
           startFrom: Option[Persistent],
           reader: Reader)(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] =
    readFooter(reader) flatMap {
      footer =>
        readHashIndexHeader(reader, footer) flatMap {
          header =>
            find(
              matcher = matcher,
              startFrom = startFrom,
              reader = reader,
              hashIndexHeader = Some(header),
              footer = footer
            )
        }
    }

  def find(matcher: KeyMatcher.Lower,
           startFrom: Option[Persistent],
           reader: Reader)(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] =
    readFooter(reader) flatMap (find(matcher, startFrom, reader, _))

  def find(matcher: KeyMatcher.Higher,
           startFrom: Option[Persistent],
           reader: Reader)(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] =
    readFooter(reader) flatMap (find(matcher, startFrom, reader, _))

  def find(matcher: KeyMatcher.Lower,
           startFrom: Option[Persistent],
           reader: Reader,
           footer: SegmentFooter)(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] =
    Catch {
      doFindSafe(
        matcher = matcher,
        startFrom = startFrom,
        reader = reader,
        checkHashIndex = None,
        footer = footer
      )
    }

  def find(matcher: KeyMatcher.Higher,
           startFrom: Option[Persistent],
           reader: Reader,
           footer: SegmentFooter)(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] =
    Catch {
      doFindSafe(
        matcher = matcher,
        startFrom = startFrom,
        reader = reader,
        checkHashIndex = None,
        footer = footer
      )
    }

  def find(matcher: KeyMatcher.Get,
           startFrom: Option[Persistent],
           reader: Reader,
           hashIndexHeader: Option[SegmentHashIndex.Header],
           footer: SegmentFooter)(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] =
    Catch {
      doFindSafe(
        matcher = matcher,
        startFrom = startFrom,
        reader = reader,
        checkHashIndex = hashIndexHeader,
        footer = footer
      )
    }

  @tailrec
  private def doFindSafe(matcher: KeyMatcher,
                         startFrom: Option[Persistent],
                         reader: Reader,
                         checkHashIndex: Option[SegmentHashIndex.Header],
                         footer: SegmentFooter)(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] =
    if (matcher.isGet && checkHashIndex.isDefined)
      checkHashIndex match {
        case Some(hashIndexHeader) =>
          SegmentHashIndex.find[Persistent](
            key = matcher.key,
            hashIndexStartOffset = footer.hashIndexStartOffset,
            hashIndexReader = reader,
            hashIndexSize = footer.hashIndexSize,
            maxProbe = hashIndexHeader.maxProbe,
            get =
              sortedIndexOffset =>
                readNextKeyValue(
                  fromPosition = footer.sortedIndexStartOffset + sortedIndexOffset,
                  startIndexOffset = footer.sortedIndexStartOffset,
                  endIndexOffset = footer.sortedIndexEndOffset,
                  indexReader = reader,
                  valueReader = reader
                ) map (Some(_)),
            getNext =
              previous =>
                if (previous.isPrefixCompressed && previous.nextIndexSize != 0)
                  readNextKeyValue(
                    previous = previous,
                    startIndexOffset = footer.sortedIndexStartOffset,
                    endIndexOffset = footer.sortedIndexEndOffset,
                    indexReader = reader,
                    valueReader = reader
                  ) map (Some(_))
                else
                  IO.none
          ) match {
            case success @ IO.Success(Some(_)) =>
              success

            case IO.Success(None) =>
              doFindSafe(
                matcher = matcher,
                startFrom = startFrom,
                reader = reader,
                checkHashIndex = None,
                footer = footer
              )
            case IO.Failure(error) =>
              IO.Failure(error)
          }

        case None =>
          doFindSafe(
            matcher = matcher,
            startFrom = startFrom,
            reader = reader,
            checkHashIndex = None,
            footer = footer
          )
      }
    else
      startFrom match {
        case Some(startFrom) =>
          //if startFrom is the last index entry, return None.
          if (startFrom.nextIndexSize == 0)
            IO.none
          else
            readNextKeyValue(
              previous = startFrom,
              startIndexOffset = footer.sortedIndexStartOffset,
              endIndexOffset = footer.sortedIndexEndOffset,
              indexReader = reader,
              valueReader = reader
            ) flatMap {
              keyValue =>
                find(startFrom, Some(keyValue), matcher, reader, footer)
            }

        //No start from. Get the first index entry from the File and start from there.
        case None =>
          readNextKeyValue(
            fromPosition = footer.sortedIndexStartOffset,
            startIndexOffset = footer.sortedIndexStartOffset,
            endIndexOffset = footer.sortedIndexEndOffset,
            indexReader = reader,
            valueReader = reader
          ) flatMap {
            keyValue =>
              find(keyValue, None, matcher, reader, footer)
          }
      }

  @tailrec
  private def find(previous: Persistent,
                   next: Option[Persistent],
                   matcher: KeyMatcher,
                   reader: Reader,
                   footer: SegmentFooter)(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] =
    matcher(
      previous = previous,
      next = next,
      hasMore = hasMore(next getOrElse previous, footer)
    ) match {
      case MatchResult.Next =>
        val readFrom = next getOrElse previous
        readNextKeyValue(
          previous = readFrom,
          startIndexOffset = footer.sortedIndexStartOffset,
          endIndexOffset = footer.sortedIndexEndOffset,
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
        IO.none
    }

  private def hasMore(keyValue: Persistent, footer: SegmentFooter) =
    keyValue.nextIndexOffset >= 0 && keyValue.nextIndexOffset < footer.sortedIndexEndOffset
}
