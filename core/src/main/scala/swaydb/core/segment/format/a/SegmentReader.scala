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
import swaydb.core.segment.format.a.index.HashIndex
import swaydb.core.util.{Bytes, CRC32}
import swaydb.data.IO
import swaydb.data.IO._
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice._
import swaydb.data.slice.{Reader, Slice}
import swaydb.data.util.ByteSizeOf

import scala.annotation.tailrec

/**
  * All public APIs are wrapped around a try catch block because eager fetches on IO's results (.getFromHashIndex).
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
                  message = s"Corrupted Segment: Failed to getFromHashIndex bytes of length $length from offset $fromOffset",
                  cause = exception
                )
              )
            )

          case ex: Exception =>
            IO.Failure(ex)
        }
    }

  def readHashIndexHeader(reader: Reader,
                          footer: SegmentFooter): IO[HashIndex.Header] =
    reader
      .moveTo(footer.hashIndexStartOffset)
      //todo read only the header bytes.
      .read(footer.hashIndexSize)
      .flatMap {
        bytes =>
          HashIndex.readHeader(Reader(bytes))
      }

  //all these functions are wrapper with a try catch block with getFromHashIndex only to make it easier to read.
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
              //              bloomAndRangeFilterSlice map {
              //                case (bloomFilterSlice, rangeFilterSlice) =>
              //                  BloomFilter(
              //                    bloomFilterBytes = bloomFilterSlice,
              //                    rangeFilterBytes = rangeFilterSlice
              //                  ).find
              //              }
              ???
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

  def get(matcher: KeyMatcher.Get,
          startFrom: Option[Persistent],
          reader: Reader)(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] =
    readFooter(reader) flatMap {
      footer =>
        readHashIndexHeader(reader, footer) flatMap {
          header =>
            get(
              matcher = matcher,
              startFrom = startFrom,
              reader = reader,
              hashIndexHeader = Some(header),
              footer = footer
            )
        }
    }

  def lower(matcher: KeyMatcher.Lower,
            startFrom: Option[Persistent],
            reader: Reader)(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] =
    readFooter(reader) flatMap (lower(matcher, startFrom, reader, _))

  def higher(matcher: KeyMatcher.Higher,
             startFrom: Option[Persistent],
             reader: Reader)(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] =
    readFooter(reader) flatMap (higher(matcher, startFrom, reader, _))

  def lower(matcher: KeyMatcher.Lower,
            startFrom: Option[Persistent],
            reader: Reader,
            footer: SegmentFooter)(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] =
    find(
      matcher = matcher,
      startFrom = startFrom,
      reader = reader,
      footer = footer
    )

  def higher(matcher: KeyMatcher.Higher,
             startFrom: Option[Persistent],
             reader: Reader,
             footer: SegmentFooter)(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] =
    find(
      matcher = matcher,
      startFrom = startFrom,
      reader = reader,
      footer = footer
    )

  def get(matcher: KeyMatcher.Get,
          startFrom: Option[Persistent],
          reader: Reader,
          hashIndexHeader: Option[HashIndex.Header],
          footer: SegmentFooter)(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] =
    hashIndexHeader map {
      hashIndexHeader =>
        getFromHashIndex(
          matcher = matcher.toNextPrefixCompressedMatcher,
          reader = reader,
          header = hashIndexHeader,
          footer = footer
        ) flatMap {
          found =>
            //if the hashIndex hit rate is perfect means the key definitely does not exist.
            //so either the bloomFilter was disabled or it returned a false positive.
            //            if (found.isEmpty && hashIndexHeader.miss == 0 && ((footer.hasRange && hashIndexHeader.rangeIndexingEnabled) || !footer.hasRange))
            //              IO.none
            //            else //still not sure go ahead with walk find.
            //              find(
            //                matcher = matcher,
            //                //todo compare and start from nearest read key-value.
            //                startFrom = startFrom,
            //                reader = reader, footer = footer
            //              )
            ???
        }
    } getOrElse {
      find(
        matcher = matcher,
        startFrom = startFrom,
        reader = reader, footer = footer
      )
    }

  private[a] def getFromHashIndex(matcher: KeyMatcher.GetNextPrefixCompressed,
                                  reader: Reader,
                                  header: HashIndex.Header,
                                  footer: SegmentFooter)(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] =
    HashIndex.find[Persistent](
      key = matcher.key,
      hashIndexStartOffset = footer.hashIndexStartOffset,
      hashIndexReader = reader,
      hashIndexSize = footer.hashIndexSize,
      maxProbe = header.maxProbe,
      assertValue =
        sortedIndexOffset =>
          findFromIndex(
            matcher = matcher,
            fromOffset = footer.sortedIndexStartOffset + sortedIndexOffset,
            reader = reader,
            footer = footer
          ) recoverWith {
            case _ =>
              //currently there is no way to detect starting point for a key-value entry in the sorted index.
              //Read requests can be submitted to random parts of the sortedIndex depending on the index returned by the hash.
              //Hash index also itself also does not store markers for a valid start sortedIndex offset
              //that's why key-values can be read at random parts of the sorted index which can return failures.
              //too many failures are not expected because probe should disallow that. And if the Segment is actually corrupted,
              //the normal forward read of the index should catch that.
              //HashIndex is suppose to make random reads faster, if the hashIndex is too small then there is no use creating one.
              IO.none
          }
    )

  private def find(matcher: KeyMatcher,
                   startFrom: Option[Persistent],
                   reader: Reader,
                   footer: SegmentFooter)(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] =
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
              matchOrNext(
                previous = startFrom,
                next = Some(keyValue),
                matcher = matcher,
                reader = reader,
                footer = footer
              )
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
            matchOrNext(
              previous = keyValue,
              next = None,
              matcher = matcher,
              reader = reader,
              footer = footer
            )
        }
    }

  private def findFromIndex(matcher: KeyMatcher,
                            fromOffset: Int,
                            reader: Reader,
                            footer: SegmentFooter)(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] =
    readNextKeyValue(
      fromPosition = fromOffset,
      startIndexOffset = footer.sortedIndexStartOffset,
      endIndexOffset = footer.sortedIndexEndOffset,
      indexReader = reader,
      valueReader = reader
    ) flatMap {
      persistent =>
        matchOrNext(
          previous = persistent,
          next = None,
          matcher = matcher,
          reader = reader,
          footer = footer
        )
    }

  @tailrec
  private def matchOrNext(previous: Persistent,
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
            matchOrNext(
              previous = readFrom,
              next = Some(nextNextKeyValue),
              matcher = matcher,
              reader = reader,
              footer = footer
            )

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
