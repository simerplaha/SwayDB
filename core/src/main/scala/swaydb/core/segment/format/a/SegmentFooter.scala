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

package swaydb.core.segment.format.a

import swaydb.core.io.reader.Reader
import swaydb.core.segment.SegmentException.SegmentCorruptionException
import swaydb.core.segment.format.a.index.{BinarySearchIndex, BloomFilter, HashIndex, SortedIndex}
import swaydb.core.util.CRC32
import swaydb.data.IO
import swaydb.data.order.KeyOrder
import swaydb.data.slice.{Reader, Slice}
import swaydb.data.util.ByteSizeOf

object SegmentFooter {

  //all these functions are wrapper with a try catch block with get only to make it easier to read.
  def read(reader: Reader)(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[SegmentFooter] =
    try {
      val fileSize = reader.size.get.toInt
      val footerStartOffset = reader.moveTo(fileSize - ByteSizeOf.int).readInt().get
      val footerSize = fileSize - footerStartOffset
      val footerBytes = reader.moveTo(footerStartOffset).read(footerSize - ByteSizeOf.int).get
      val footerReader = Reader(footerBytes)
      val formatId = footerReader.readIntUnsigned().get
      if (formatId != SegmentWriter.formatId) {
        val message = s"Invalid Segment formatId: $formatId. Expected: ${SegmentWriter.formatId}"
        return IO.Failure(IO.Error.Fatal(SegmentCorruptionException(message = message, cause = new Exception(message))))
      }
      assert(formatId == SegmentWriter.formatId, s"Invalid Segment formatId: $formatId. Expected: ${SegmentWriter.formatId}")
      val createdInLevel = footerReader.readIntUnsigned().get
      val hasGroup = footerReader.readBoolean().get
      val hasRange = footerReader.readBoolean().get
      val hasPut = footerReader.readBoolean().get
      val keyValueCount = footerReader.readIntUnsigned().get
      val bloomFilterItemsCount = footerReader.readIntUnsigned().get
      val expectedCRC = footerReader.readLong().get
      val crcBytes = footerBytes.take(SegmentWriter.crcBytes)
      val crc = CRC32.forBytes(crcBytes)
      if (expectedCRC != crc) {
        IO.Failure(SegmentCorruptionException(s"Corrupted Segment: CRC Check failed. $expectedCRC != $crc", new Exception("CRC check failed.")))
      } else {
        val sortedIndexOffset =
          SortedIndex.Offset(
            size = footerReader.readIntUnsigned().get,
            start = footerReader.readIntUnsigned().get
          )

        val hashIndexSize = footerReader.readIntUnsigned().get
        val hashIndexOffset =
          if (hashIndexSize == 0)
            None
          else
            Some(
              HashIndex.Offset(
                start = footerReader.readIntUnsigned().get,
                size = hashIndexSize
              )
            )

        val binarySearchIndexSize = footerReader.readIntUnsigned().get
        val binarySearchIndexOffset =
          if (binarySearchIndexSize == 0)
            None
          else
            Some(
              BinarySearchIndex.Offset(
                start = footerReader.readIntUnsigned().get,
                size = binarySearchIndexSize
              )
            )

        val bloomFilterSize = footerReader.readIntUnsigned().get
        val bloomFilterOffset =
          if (hashIndexSize == 0)
            None
          else
            Some(
              BloomFilter.Offset(
                start = footerReader.readIntUnsigned().get,
                size = bloomFilterSize
              )
            )

        IO.Success(
          SegmentFooter(
            sortedIndexOffset = sortedIndexOffset,
            hashIndexOffset = hashIndexOffset,
            binarySearchIndexOffset = binarySearchIndexOffset,
            bloomFilterOffset = bloomFilterOffset,
            keyValueCount = keyValueCount,
            createdInLevel = createdInLevel,
            bloomFilterItemsCount = bloomFilterItemsCount,
            hasRange = hasRange,
            hasGroup = hasGroup,
            hasPut = hasPut
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
}

private[core] case class SegmentFooter(sortedIndexOffset: SortedIndex.Offset,
                                       hashIndexOffset: Option[HashIndex.Offset],
                                       binarySearchIndexOffset: Option[BinarySearchIndex.Offset],
                                       bloomFilterOffset: Option[BloomFilter.Offset],
                                       keyValueCount: Int,
                                       createdInLevel: Int,
                                       bloomFilterItemsCount: Int,
                                       hasRange: Boolean,
                                       hasGroup: Boolean,
                                       hasPut: Boolean)