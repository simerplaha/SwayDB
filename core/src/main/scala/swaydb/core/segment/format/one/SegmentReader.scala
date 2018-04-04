/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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

package swaydb.core.segment.format.one

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.data.{KeyValue, Persistent, Transient}
import swaydb.core.io.reader.Reader
import swaydb.core.map.serializer.RangeValueSerializer
import swaydb.core.segment.SegmentException.SegmentCorruptionException
import swaydb.core.util.BloomFilterUtil._
import swaydb.core.util.TryUtil._
import swaydb.core.util.{ByteUtilCore, CRC32}
import swaydb.data.slice.Slice._
import swaydb.data.slice.{Reader, Slice}
import swaydb.data.util.ByteSizeOf

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

/**
  * All public APIs are wrapped around a try catch block because eager fetches on Try's results (.get).
  * Eventually need to re-factor this code to use flatMap. Leaving as it is now because it's easier to read.
  */
private[core] object SegmentReader extends LazyLogging {

  private def readNextKeyValue[P <: Persistent](previous: P,
                                                         endIndexOffset: Int,
                                                         reader: Reader,
                                                         onCreate: (Slice[Byte], Int, Int, Int, Int) => P,
                                                         onRange: (Int, Slice[Byte], Int, Int, Int, Int) => Try[P],
                                                         onDelete: (Slice[Byte], Int, Int) => P): Try[P] = {
    reader moveTo previous.nextIndexOffset
    readNextKeyValue(
      indexEntrySizeMayBe = Some(previous.nextIndexSize),
      adjustNextIndexOffsetBy = 0,
      endIndexOffset = endIndexOffset,
      reader = reader,
      previous = Some(previous),
      onCreate = onCreate,
      onRange = onRange,
      onDelete = onDelete
    )
  }

  private def readNextKeyValue[P <: Persistent](fromPosition: Int,
                                                endIndexOffset: Int,
                                                reader: Reader,
                                                onCreate: (Slice[Byte], Int, Int, Int, Int) => P,
                                                onRange: (Int, Slice[Byte], Int, Int, Int, Int) => Try[P],
                                                onDelete: (Slice[Byte], Int, Int) => P): Try[P] = {
    reader moveTo fromPosition
    readNextKeyValue(
      indexEntrySizeMayBe = None,
      adjustNextIndexOffsetBy = 0,
      endIndexOffset = endIndexOffset,
      reader = reader,
      previous = None,
      onCreate = onCreate,
      onRange = onRange,
      onDelete = onDelete
    )
  }

  //Pre-requisite: The position of the index on the reader should be set.
  private def readNextKeyValue[P <: Persistent](indexEntrySizeMayBe: Option[Int],
                                                adjustNextIndexOffsetBy: Int, //the reader could be a sub slice. This is used to adjust next indexOffset size
                                                endIndexOffset: Int,
                                                reader: Reader,
                                                previous: Option[P],
                                                onCreate: (Slice[Byte], Int, Int, Int, Int) => P,
                                                onRange: (Int, Slice[Byte], Int, Int, Int, Int) => Try[P],
                                                onDelete: (Slice[Byte], Int, Int) => P): Try[P] =
    try {
      val positionBeforeRead = reader.getPosition
      //size of the index entry to read
      val indexSize =
        indexEntrySizeMayBe match {
          case Some(indexEntrySize) =>
            reader skip ByteUtilCore.sizeUnsignedInt(indexEntrySize)
            indexEntrySize

          case None =>
            reader.readIntUnsigned().get
        }

      val (bytesToRead, hasMore) = {
        val bytesRead = reader.getPosition - positionBeforeRead
        val remainingBytesToRead = indexSize - bytesRead
        val hasNext = reader.getPosition + remainingBytesToRead < endIndexOffset
        if (hasNext)
        //also read a minimum of 5 bytes to get the nextIndexSize in the same seek.
          (remainingBytesToRead + 5, true)
        else
          (remainingBytesToRead, false)
      }

      val indexEntryReader = Reader((reader read bytesToRead).get)

      val id = indexEntryReader.readIntUnsigned().get
      val commonBytes = indexEntryReader.readIntUnsigned().get
      val keyLength = indexEntryReader.readIntUnsigned().get
      var key = (indexEntryReader read keyLength).get
      //if this key has common bytes, build the full key.
      if (commonBytes != 0)
        previous foreach {
          previous =>
            val missingCommonBytes = previous.key.slice(0, commonBytes - 1)
            val fullKey = new Array[Byte](commonBytes + key.size)
            var i = 0
            //could use fold here but this code is crucial for read performance.
            while (i < commonBytes) {
              fullKey(i) = missingCommonBytes(i)
              i += 1
            }
            var x = 0
            while (x < keyLength) {
              fullKey(i) = key(x)
              x += 1
              i += 1
            }
            key = Slice(fullKey)
        }

      //The above fetches another 5 bytes (unsigned int) along with previous index entry.
      //These 5 bytes contains the next index's size. Here the next key-values indexSize and indexOffset are read.
      def nextIndexSizeAndOffset: (Int, Int) =
        if (hasMore)
          (indexEntryReader.readIntUnsigned().get, reader.getPosition - 5 + adjustNextIndexOffsetBy)
        else
          (0, -1)

      val valueLength = indexEntryReader.readIntUnsigned().get

      if (id == Transient.Put.id) {
        val valueOffset = if (valueLength == 0) 0 else indexEntryReader.readIntUnsigned().get
        val (nextIndexSize, nextIndexOffset) = nextIndexSizeAndOffset
        Success(onCreate(key, valueLength, valueOffset, nextIndexOffset, nextIndexSize))
      } else if (id == Transient.Remove.id) {
        val (nextIndexSize, nextIndexOffset) = nextIndexSizeAndOffset
        Success(onDelete(key, nextIndexOffset, nextIndexSize))
      } else if (RangeValueSerializer.isRangeValue(id)) {
        val valueOffset = if (valueLength == 0) 0 else indexEntryReader.readIntUnsigned().get
        val (nextIndexSize, nextIndexOffset) = nextIndexSizeAndOffset
        onRange(id, key, valueLength, valueOffset, nextIndexOffset, nextIndexSize)
      } else {
        throw SegmentCorruptionException(s"Invalid indexEntry ID: $id. Segment file is corrupted.", new Exception("Segment corruption"))
      }
    } catch {
      case exception: Exception =>
        exception match {
          case _: ArrayIndexOutOfBoundsException | _: IndexOutOfBoundsException | _: IllegalArgumentException | _: NegativeArraySizeException =>
            val atPosition: String = indexEntrySizeMayBe.map(size => s" of size $size") getOrElse ""
            Failure(
              SegmentCorruptionException(
                s"Corrupted Segment: Failed to read index entry at reader position ${reader.getPosition}$atPosition}",
                exception
              )
            )

          case ex: Exception =>
            Failure(ex)
        }
    }

  def readAll(footer: SegmentFooter,
              reader: Reader,
              addTo: Option[Slice[KeyValue.ReadOnly]] = None): Try[Slice[KeyValue.ReadOnly]] =
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
            endIndexOffset = endIndexOffset,
            reader = indexOnlyReader,
            //user entries.lastOption instead of previousMayBe because, addTo might already be pre-populated and the
            //last entry would of bethe.
            previous = previousMayBe,
            onCreate = Persistent.Put(reader.copy(), previousMayBe.map(_.nextIndexOffset).getOrElse(footer.startIndexOffset)),
            onRange = Persistent.Range(reader.copy(), previousMayBe.map(_.nextIndexOffset).getOrElse(footer.startIndexOffset)),
            onDelete = Persistent.Remove(previousMayBe.map(_.nextIndexOffset).getOrElse(footer.startIndexOffset))
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
            Failure(SegmentCorruptionException(s"Corrupted Segment: Failed to read index bytes", exception))

          case ex: Exception =>
            Failure(ex)
        }
    }

  def getValue(valueOffset: Int, valueLength: Int, reader: Reader): Try[Option[Slice[Byte]]] =
    try {
      if (valueLength == 0)
        Success(None)
      else
        (reader.copy() moveTo valueOffset read valueLength).map(Some(_))
    } catch {
      case exception: Exception =>
        exception match {
          case _: ArrayIndexOutOfBoundsException | _: IndexOutOfBoundsException | _: IllegalArgumentException | _: NegativeArraySizeException =>
            Failure(
              SegmentCorruptionException(
                s"Corrupted Segment: Failed to get values bytes of length $valueLength from offset $valueOffset",
                exception
              )
            )

          case ex: Exception =>
            Failure(ex)
        }
    }

  def readFooter(reader: Reader): Try[SegmentFooter] = {
    try {
      val fileSize = reader.size.get
      val footerSize = reader.moveTo(fileSize - ByteSizeOf.int).readInt().get
      val footerBytes = reader.moveTo(fileSize - footerSize - ByteSizeOf.int).read(footerSize)
      val footerReader = Reader(footerBytes.get)
      footerReader.readIntUnsigned().get
      val hasRange = if (footerReader.get().get == 1) true else false
      val indexStartOffset = footerReader.readIntUnsigned().get
      val expectedCRC = footerReader.readLong().get
      val keyValueCount = footerReader.readIntUnsigned().get
      val bloomFilterSize = footerReader.readIntUnsigned().get
      val bloomFilterSlice =
        if (bloomFilterSize == 0)
          None
        else
          Some(footerReader.read(bloomFilterSize).get)

      val crcBytes = reader.moveTo(indexStartOffset).read(SegmentWriter.crcBytes).get
      val crc = CRC32.forBytes(crcBytes)
      if (expectedCRC != crc) {
        Failure(SegmentCorruptionException(s"Corrupted Segment: CRC Check failed. $expectedCRC != $crc", new Exception("CRC check failed.")))
      } else {
        val indexEndOffset = fileSize.toInt - footerSize - ByteSizeOf.int - 1
        Success(SegmentFooter(expectedCRC, indexStartOffset, indexEndOffset, keyValueCount, hasRange = hasRange, bloomFilterSlice.map(_.toBloomFilter)))
      }
    } catch {
      case exception: Exception =>
        exception match {
          case _: ArrayIndexOutOfBoundsException | _: IndexOutOfBoundsException | _: IllegalArgumentException | _: NegativeArraySizeException =>
            Failure(SegmentCorruptionException("Corrupted Segment: Failed to read footer bytes", exception))

          case ex: Exception =>
            Failure(ex)
        }
    }
  }

  def find(matcher: KeyMatcher,
           startFrom: Option[Persistent],
           reader: Reader): Try[Option[Persistent]] =
    readFooter(reader) flatMap (find(matcher, startFrom, reader, _))

  def find(matcher: KeyMatcher,
           startFrom: Option[Persistent],
           reader: Reader,
           footer: SegmentFooter): Try[Option[Persistent]] =
    try {
      startFrom match {
        case Some(startFrom) =>
          //if startFrom is the last index entry, return None.
          if (startFrom.nextIndexSize == 0)
            Success(None)
          else
            readNextKeyValue(
              previous = startFrom,
              endIndexOffset = footer.endIndexOffset,
              reader = reader,
              onCreate = Persistent.Put(reader, startFrom.nextIndexOffset),
              onRange = Persistent.Range(reader, startFrom.nextIndexOffset),
              onDelete = Persistent.Remove(startFrom.nextIndexOffset)
            ) flatMap {
              keyValue =>
                find(startFrom, Some(keyValue), matcher, reader, footer)
            }

        //No start from. Get the first index entry from the File and start from there.
        case None =>
          readNextKeyValue(
            fromPosition = footer.startIndexOffset,
            endIndexOffset = footer.endIndexOffset,
            reader = reader,
            onCreate = Persistent.Put(reader, footer.startIndexOffset),
            onRange = Persistent.Range(reader, footer.startIndexOffset),
            onDelete = Persistent.Remove(footer.startIndexOffset)
          ) flatMap {
            keyValue =>
              find(keyValue, None, matcher, reader, footer)
          }
      }
    } catch {
      case ex: Exception =>
        Failure(ex)
    }

  @tailrec
  private def find(previous: Persistent,
                   next: Option[Persistent],
                   matcher: KeyMatcher,
                   reader: Reader,
                   footer: SegmentFooter): Try[Option[Persistent]] =
    matcher(previous, next, hasMore = hasMore(next getOrElse previous, footer)) match {
      case MatchResult.Next =>
        val readFrom = next getOrElse previous
        readNextKeyValue(
          previous = readFrom,
          endIndexOffset = footer.endIndexOffset,
          reader = reader,
          onCreate = Persistent.Put(reader, readFrom.nextIndexOffset),
          onRange = Persistent.Range(reader, readFrom.nextIndexOffset),
          onDelete = Persistent.Remove(readFrom.nextIndexOffset)
        ) match {
          case Success(nextNextKeyValue) =>
            find(readFrom, Some(nextNextKeyValue), matcher, reader, footer)

          case Failure(exception) =>
            Failure(exception)
        }

      case MatchResult.Matched(keyValue) =>
        Success(Some(keyValue))

      case MatchResult.Stop =>
        Success(None)

    }

  private def hasMore(keyValue: Persistent, footer: SegmentFooter) =
    keyValue.nextIndexOffset >= 0 && keyValue.nextIndexOffset < footer.endIndexOffset

}
