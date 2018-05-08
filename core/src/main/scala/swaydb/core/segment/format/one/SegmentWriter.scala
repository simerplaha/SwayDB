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

import bloomfilter.mutable.BloomFilter
import com.typesafe.scalalogging.LazyLogging
import swaydb.core.data.KeyValue
import swaydb.core.segment.Segment
import swaydb.core.util.BloomFilterUtil._
import swaydb.core.util.CRC32
import swaydb.data.slice.Slice
import swaydb.data.slice.Slice._

import scala.concurrent.duration.Deadline
import scala.util.{Success, Try}

private[core] object SegmentWriter extends LazyLogging {

  val crcBytes: Int = 7

  /**
    * Rules for creating bloom filters
    *
    * - If key-values contains a Remove range - bloom filters are not created
    * - If key-value contains a Range - a flag is added to Appendix
    * - if key-value contains an Update range - get does not consult
    */

  def toSlice(keyValues: Iterable[KeyValue.WriteOnly], bloomFilterFalsePositiveRate: Double): Try[(Slice[Byte], Option[Deadline])] = {
    if (keyValues.isEmpty) {
      Success(Slice.create[Byte](0), None)
    } else {
      Try {
        val bloomFilter =
          if (keyValues.last.stats.hasRemoveRange)
            None
          else
            Some(BloomFilter[Slice[Byte]](keyValues.size, bloomFilterFalsePositiveRate))

        val slice = Slice.create[Byte](keyValues.last.stats.segmentSize)
        var deadline: Option[Deadline] = None

        val (valuesSlice, indexAndFooterSlice) = slice splitAt keyValues.last.stats.segmentValuesSize
        keyValues foreach {
          keyValue =>
            keyValue.getOrFetchValue flatMap {
              valueMayBe =>
                Segment.getNearestDeadline(deadline, keyValue) map {
                  nearestDeadline =>
                    deadline = nearestDeadline

                    bloomFilter.foreach(_ add keyValue.key)

                    indexAndFooterSlice addIntUnsigned keyValue.stats.thisKeyValuesIndexSizeWithoutFooter
                    indexAndFooterSlice addIntUnsigned keyValue.id
                    indexAndFooterSlice addIntUnsigned keyValue.stats.commonBytes
                    indexAndFooterSlice addIntUnsigned keyValue.stats.keyWithoutCommonBytes.size
                    indexAndFooterSlice addAll keyValue.stats.keyWithoutCommonBytes
                    keyValue match {
                      case single: KeyValue.WriteOnly.Fixed =>
                        indexAndFooterSlice addLongUnsigned single.deadline.map(_.time.toNanos).getOrElse(0L)
                      case _: KeyValue.WriteOnly.Range =>
                        indexAndFooterSlice addLongUnsigned 0L
                    }

                    valueMayBe match {
                      case Some(value) if value.size > 0 =>
                        assert(valuesSlice.written == keyValue.stats.valueOffset, s"Value offset is incorrect. actual: ${valuesSlice.written} - expected: ${keyValue.stats.valueOffset}")
                        indexAndFooterSlice addIntUnsigned keyValue.stats.valueLength
                        indexAndFooterSlice addIntUnsigned valuesSlice.written

                        valuesSlice addAll value //value

                      case _ =>
                        indexAndFooterSlice addIntUnsigned 0
                    }
                }
            }
        }

        val footerStart = indexAndFooterSlice.written
        //this is a placeholder to store the format type of the Segment file written.
        //currently there is only one format. So this is hardcoded but if there are a new file format then
        //SegmentWriter and SegmentReader should be changed to be type classes with unique format types ids.
        indexAndFooterSlice addIntUnsigned 1
        if (keyValues.last.stats.hasRange) indexAndFooterSlice addByte 1 else indexAndFooterSlice addByte 0
        indexAndFooterSlice addIntUnsigned indexAndFooterSlice.fromOffset
        indexAndFooterSlice addLong CRC32.forBytes(indexAndFooterSlice.take(SegmentWriter.crcBytes))
        indexAndFooterSlice addIntUnsigned keyValues.size
        bloomFilter match {
          case Some(bloomFilter) =>
            val bloomFilterBytes = bloomFilter.toBytes
            indexAndFooterSlice addIntUnsigned bloomFilterBytes.length
            indexAndFooterSlice addAll bloomFilterBytes
          case None =>
            indexAndFooterSlice addIntUnsigned 0
        }
        indexAndFooterSlice addInt (indexAndFooterSlice.written - footerStart)
        assert(valuesSlice.isFull, "Values slice is not full")
        assert(indexAndFooterSlice.isFull, s"Index and footer slice is not full. Size: ${indexAndFooterSlice.size} - Written: ${indexAndFooterSlice.written}")
        val sliceArray = slice.toArray
        assert(keyValues.last.stats.segmentSize == sliceArray.length, s"Invalid segment size. actual: ${sliceArray.length} - expected: ${keyValues.last.stats.segmentSize}")
        (Slice(sliceArray), deadline)
      }
    }
  }
}