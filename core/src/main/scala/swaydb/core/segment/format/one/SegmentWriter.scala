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
import swaydb.core.data.{KeyValue, SegmentEntry, Transient}
import swaydb.core.util.BloomFilterUtil._
import swaydb.core.util.CRC32
import swaydb.data.slice.Slice
import swaydb.data.slice.Slice._

import scala.util.{Success, Try}

private[core] object SegmentWriter extends LazyLogging {

  val crcBytes: Int = 7

  def toSlice(keyValues: Iterable[KeyValue.WriteOnly], bloomFilterFalsePositiveRate: Double): Try[Slice[Byte]] = {
    if (keyValues.isEmpty) {
      Success(Slice.create[Byte](0))
    } else {
      Try {
        val bloomFilter = BloomFilter[Slice[Byte]](keyValues.size, bloomFilterFalsePositiveRate)

        val slice = Slice.create[Byte](keyValues.last.stats.segmentSize)

        val (valuesSlice, indexAndFooterSlice) = slice splitAt keyValues.last.stats.segmentValuesSize
        keyValues foreach {
          keyValue =>
            val valueMayBe =
              keyValue match {
                case transient: Transient =>
                  Success(transient.value)

                case persistent: SegmentEntry =>
                  persistent.getOrFetchValue
              }

            valueMayBe map {
              valueMayBe =>
                bloomFilter add keyValue.key

                indexAndFooterSlice addIntUnsigned keyValue.stats.thisKeyValuesIndexSizeWithoutFooter
                indexAndFooterSlice addIntUnsigned keyValue.id
                indexAndFooterSlice addIntUnsigned keyValue.stats.commonBytes
                indexAndFooterSlice addIntUnsigned keyValue.stats.keyWithoutCommonBytes.size
                indexAndFooterSlice addAll keyValue.stats.keyWithoutCommonBytes

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

        val footerStart = indexAndFooterSlice.written
        //this is a placeholder to store the format type of the Segment file written.
        //currently there is only one format. So this is hardcoded but if there are a new file format then
        //SegmentWriter and SegmentReader should be changed to be type classes with unique format types ids.
        indexAndFooterSlice addIntUnsigned 1
        indexAndFooterSlice addIntUnsigned indexAndFooterSlice.fromOffset
        indexAndFooterSlice addLong CRC32.forBytes(indexAndFooterSlice.take(SegmentWriter.crcBytes))
        indexAndFooterSlice addIntUnsigned keyValues.size
        indexAndFooterSlice addAll bloomFilter.toBytes
        indexAndFooterSlice addInt (indexAndFooterSlice.written - footerStart)
        assert(valuesSlice.isFull, "Values slice is not full")
        assert(indexAndFooterSlice.isFull, s"Index and footer slice is not full. Size: ${indexAndFooterSlice.size} - Written: ${indexAndFooterSlice.written}")
        val sliceArray = slice.toArray
        assert(keyValues.last.stats.segmentSize == sliceArray.length, s"Invalid segment size. actual: ${sliceArray.length} - expected: ${keyValues.last.stats.segmentSize}")
        Slice(sliceArray)
      }
    }
  }
}