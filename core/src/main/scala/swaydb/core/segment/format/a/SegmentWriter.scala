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

import bloomfilter.mutable.BloomFilter
import com.typesafe.scalalogging.LazyLogging
import swaydb.core.data.{KeyValue, Transient}
import swaydb.core.segment.Segment
import swaydb.core.util.BloomFilterUtil._
import swaydb.core.util.{BloomFilterUtil, CRC32}
import swaydb.core.util.PipeOps._
import swaydb.data.slice.Slice
import swaydb.data.slice.Slice._

import scala.concurrent.duration.Deadline
import swaydb.data.io.IO
import swaydb.data.io.IO._

private[core] object SegmentWriter extends LazyLogging {

  val formatId = 0

  val crcBytes: Int = 7

  private def getNearestDeadlineAndAddToBloomFilter(bloomFilter: Option[BloomFilter[Slice[Byte]]],
                                                    deadline: Option[Deadline],
                                                    keyValue: KeyValue.WriteOnly): Option[Deadline] =
    keyValue match {
      case group: Transient.Group =>
        group.keyValues.foldLeft(deadline) {
          case (nearestDeadline, keyValue) =>
            getNearestDeadlineAndAddToBloomFilter(bloomFilter, nearestDeadline, keyValue)
        }
      case _: Transient.Put | _: Transient.Remove | _: Transient.Update | _: Transient.Range | _: Transient.Function | _: Transient.PendingApply =>
        bloomFilter foreach (_ add keyValue.key)
        Segment.getNearestDeadline(deadline, keyValue)
    }

  def write(keyValues: Iterable[KeyValue.WriteOnly],
            indexSlice: Slice[Byte],
            valuesSlice: Slice[Byte],
            bloomFilter: Option[BloomFilter[Slice[Byte]]]): IO[Option[Deadline]] =
    keyValues.tryFoldLeft(Option.empty[Deadline]) {
      case (deadline, keyValue) =>
        write(
          keyValue = keyValue,
          indexSlice = indexSlice,
          valuesSlice = valuesSlice
        ) map {
          _ =>
            getNearestDeadlineAndAddToBloomFilter(bloomFilter, deadline, keyValue)
        }
    } flatMap {
      result =>
        //ensure that all the slices are full.
        if (!indexSlice.isFull)
          IO.Failure(new Exception(s"indexSlice is not full actual: ${indexSlice.written} - expected: ${indexSlice.size}"))
        else if (!valuesSlice.isFull)
          IO.Failure(new Exception(s"valuesSlice is not full actual: ${valuesSlice.written} - expected: ${valuesSlice.size}"))
        else
          IO.Success(result)
    }

  private def write(keyValue: KeyValue.WriteOnly,
                    indexSlice: Slice[Byte],
                    valuesSlice: Slice[Byte]): IO[Unit] =
    IO {
      indexSlice addIntUnsigned keyValue.stats.keySize
      indexSlice addAll keyValue.indexEntryBytes
      keyValue.valueEntryBytes foreach (valuesSlice addAll _)
    }

  /**
    * Rules for creating bloom filters
    *
    * If key-values contains:
    * 1. A Remove range - bloom filters are not created because 'mightContain' checks bloomFilters only and bloomFilters
    * do not have range scans. BloomFilters are still created for Update ranges because even if boomFilter returns false,
    * 'mightContain' will continue looking for the key in lower Levels but a remove Range should always return false.
    *
    * 2. Any other Range - a flag is added to Appendix indicating that the Segment contains a Range key-value so that
    * Segment reads can take appropriate steps to fetch the right range key-value.
    */
  def write(keyValues: Iterable[KeyValue.WriteOnly],
            bloomFilterFalsePositiveRate: Double): IO[(Slice[Byte], Option[Deadline])] =
    if (keyValues.isEmpty)
      IO.Success(Slice.emptyBytes, None)
    else {
      val bloomFilter = BloomFilterUtil.initBloomFilter(keyValues, bloomFilterFalsePositiveRate)

      val slice = Slice.create[Byte](keyValues.last.stats.segmentSize)

      val (valuesSlice, indexSlice, footerSlice) =
        slice.splitAt(keyValues.last.stats.segmentValuesSize) ==> {
          case (valuesSlice, indexAndFooterSlice) =>
            val (indexSlice, footerSlice) = indexAndFooterSlice splitAt keyValues.last.stats.indexSize
            (valuesSlice, indexSlice, footerSlice)
        }

      write(
        keyValues = keyValues,
        indexSlice = indexSlice,
        valuesSlice = valuesSlice,
        bloomFilter = bloomFilter
      ) flatMap {
        nearestDeadline =>
          IO {
            //this is a placeholder to store the format type of the Segment file written.
            //currently there is only one format. So this is hardcoded but if there are a new file format then
            //SegmentWriter and SegmentReader should be changed to be type classes with unique format types ids.
            footerSlice addIntUnsigned SegmentWriter.formatId
            footerSlice addBoolean keyValues.last.stats.hasRange
            footerSlice addBoolean keyValues.last.stats.hasPut
            footerSlice addIntUnsigned indexSlice.fromOffset

            //do CRC
            var indexBytesToCRC = indexSlice.take(SegmentWriter.crcBytes)
            if (indexBytesToCRC.size < SegmentWriter.crcBytes) //if index does not have enough bytes, fill remaining from the footer.
              indexBytesToCRC = indexBytesToCRC append footerSlice.take(SegmentWriter.crcBytes - indexBytesToCRC.size)
            assert(indexBytesToCRC.size == SegmentWriter.crcBytes, s"Invalid CRC bytes size: ${indexBytesToCRC.size}. Required: ${SegmentWriter.crcBytes}")
            footerSlice addLong CRC32.forBytes(indexBytesToCRC)

            //here the top Level key-values are used instead of Group's internal key-values because Group's internal key-values
            //are read when the Group key-value is read.
            footerSlice addIntUnsigned keyValues.size
            //total number of actual key-values grouped or un-grouped
            footerSlice addIntUnsigned keyValues.last.stats.bloomFilterItemsCount
            bloomFilter match {
              case Some(bloomFilter) =>
                val bloomFilterBytes = bloomFilter.toBytes
                footerSlice addIntUnsigned bloomFilterBytes.length
                footerSlice addAll bloomFilterBytes
              case None =>
                footerSlice addIntUnsigned 0
            }
            footerSlice addInt footerSlice.size
            assert(footerSlice.isFull, s"footerSlice is not full. Size: ${footerSlice.size} - Written: ${footerSlice.written}")
            val segmentArray = slice.toArray
            assert(keyValues.last.stats.segmentSize == segmentArray.length, s"Invalid segment size. actual: ${segmentArray.length} - expected: ${keyValues.last.stats.segmentSize}")
            (Slice(segmentArray), nearestDeadline)
          }
      }
    }
}
