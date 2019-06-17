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
import swaydb.core.data.KeyValue
import swaydb.core.segment.Segment
import swaydb.core.util.PipeOps._
import swaydb.core.util.{BloomFilter, Bytes, CRC32}
import swaydb.data.IO
import swaydb.data.IO._
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.data.slice.Slice._

import scala.annotation.tailrec
import scala.concurrent.duration.Deadline

private[core] object SegmentWriter extends LazyLogging {

  val formatId = 0

  val crcBytes: Int = 7

  def writeToHashIndexAndBloomFilterBytes(keyValue: KeyValue.WriteOnly,
                                          hashIndex: Option[SegmentHashIndex.WriteResult],
                                          maxProbe: Int,
                                          bloom: Option[BloomFilter],
                                          currentNearestDeadline: Option[Deadline]): IO[Option[Deadline]] = {

    def writeToBloomAndHashIndex(rootGroup: Option[KeyValue.WriteOnly.Group],
                                 keyValue: KeyValue.WriteOnly): Unit =
      keyValue match {
        case childGroup: KeyValue.WriteOnly.Group =>
          writesToBloomAndHashIndex(
            rootGroup = rootGroup,
            keyValues = childGroup.keyValues
          )

        case otherKeyValue: KeyValue.WriteOnly.Range =>
          bloom foreach (_.add(otherKeyValue.key, otherKeyValue.toKey))
          hashIndex foreach {
            hashIndexResult =>
              val thisKeyValuesHashIndex = rootGroup.map(_.stats.thisKeyValuesHashIndexesSortedIndexOffset).getOrElse(otherKeyValue.stats.thisKeyValuesHashIndexesSortedIndexOffset)
              SegmentHashIndex.write(
                key = keyValue.key,
                sortedIndexOffset = thisKeyValuesHashIndex,
                bytes = hashIndexResult.bytes,
                maxProbe = maxProbe
              ) map {
                winner =>
                  if (winner)
                    hashIndexResult.hit += 1
                  else
                    hashIndexResult.miss += 1

                  hashIndexResult
              }
          }

        case otherKeyValue: KeyValue.WriteOnly =>
          bloom foreach (_.add(otherKeyValue.key))
          hashIndex foreach {
            hashIndexResult =>
              val thisKeyValuesHashIndex = rootGroup.map(_.stats.thisKeyValuesHashIndexesSortedIndexOffset).getOrElse(otherKeyValue.stats.thisKeyValuesHashIndexesSortedIndexOffset)
              SegmentHashIndex.write(
                key = keyValue.key,
                sortedIndexOffset = thisKeyValuesHashIndex,
                bytes = hashIndexResult.bytes,
                maxProbe = maxProbe
              ) map {
                winner =>
                  if (winner)
                    hashIndexResult.hit += 1
                  else
                    hashIndexResult.miss += 1

                  hashIndexResult
              }
          }
      }

    @tailrec
    def writesToBloomAndHashIndex(rootGroup: Option[KeyValue.WriteOnly.Group],
                                  keyValues: Slice[KeyValue.WriteOnly]): Unit =
      keyValues.headOption match {
        case Some(keyValue) =>
          writeToBloomAndHashIndex(rootGroup, keyValue)
          writesToBloomAndHashIndex(rootGroup, keyValues.drop(1))

        case None =>
          ()
      }

    @tailrec
    def start(keyValues: Slice[KeyValue.WriteOnly], nearestDeadline: Option[Deadline]): Option[Deadline] =
      keyValues.headOption match {
        case group @ Some(rootGroup: KeyValue.WriteOnly.Group) =>
          val nextNearestDeadline = Segment.getNearestDeadline(nearestDeadline, rootGroup)
          //run writeBloomFilters only if bloomFilters is defined. To keep the stack small do not pass BloomFilter
          //to the function because a Segment can contain many key-values.
          if (hashIndex.isDefined || bloom.isDefined)
            writesToBloomAndHashIndex(
              rootGroup = group.asInstanceOf[Some[KeyValue.WriteOnly.Group]],
              keyValues = rootGroup.keyValues
            )

          start(keyValues.drop(1), nextNearestDeadline)

        case Some(otherKeyValue) =>
          if (hashIndex.isDefined || bloom.isDefined)
            writeToBloomAndHashIndex(
              rootGroup = None,
              keyValue = otherKeyValue
            )
          val nextNearestDeadline = Segment.getNearestDeadline(nearestDeadline, otherKeyValue)
          start(keyValues.drop(1), nextNearestDeadline)

        case None =>
          hashIndex foreach SegmentHashIndex.writeHeader
          nearestDeadline
      }

    IO(
      start(
        keyValues = Slice(keyValue),
        nearestDeadline = currentNearestDeadline
      )
    )
  }

  private def writeToIndexAndValueBytes(keyValue: KeyValue.WriteOnly,
                                        sortedIndexSlice: Slice[Byte],
                                        valuesSlice: Slice[Byte]): IO[Unit] =
    IO {
      sortedIndexSlice addIntUnsigned keyValue.stats.keySize
      sortedIndexSlice addAll keyValue.indexEntryBytes
      keyValue.valueEntryBytes foreach (valuesSlice addAll _)
    }

  private def writeSegmentBytes(keyValue: KeyValue.WriteOnly,
                                sortedIndexSlice: Slice[Byte],
                                valuesSlice: Slice[Byte],
                                hashIndex: Option[SegmentHashIndex.WriteResult],
                                maxProbe: Int,
                                bloomFilter: Option[BloomFilter],
                                deadline: Option[Deadline]) =
    writeToIndexAndValueBytes(
      keyValue = keyValue,
      sortedIndexSlice = sortedIndexSlice,
      valuesSlice = valuesSlice
    ) flatMap {
      _ =>
        writeToHashIndexAndBloomFilterBytes(
          keyValue = keyValue,
          bloom = bloomFilter,
          hashIndex = hashIndex,
          currentNearestDeadline = deadline,
          maxProbe = maxProbe
        )
    }

  def write(keyValues: Iterable[KeyValue.WriteOnly],
            sortedIndexSlice: Slice[Byte],
            valuesSlice: Slice[Byte],
            hashIndex: Option[SegmentHashIndex.WriteResult],
            maxProbe: Int,
            bloomFilter: Option[BloomFilter]): IO[Option[Deadline]] =
    keyValues.foldLeftIO(Option.empty[Deadline]) {
      case (deadline, keyValue) =>
        writeSegmentBytes(
          keyValue = keyValue,
          sortedIndexSlice = sortedIndexSlice,
          valuesSlice = valuesSlice,
          hashIndex = hashIndex,
          maxProbe = maxProbe,
          bloomFilter = bloomFilter,
          deadline = deadline
        )
    } flatMap {
      deadline =>
        //ensure that all the slices are full.
        if (!sortedIndexSlice.isFull)
          IO.Failure(new Exception(s"indexSlice is not full actual: ${sortedIndexSlice.written} - expected: ${sortedIndexSlice.size}"))
        else if (!valuesSlice.isFull)
          IO.Failure(new Exception(s"valuesSlice is not full actual: ${valuesSlice.written} - expected: ${valuesSlice.size}"))
        else
          hashIndex map {
            hashIndex =>
              if (!hashIndex.bytes.isFull)
                IO.Failure(new Exception(s"hashIndexSlice is not full actual: ${hashIndex.bytes.written} - expected: ${hashIndex.bytes.size}"))
              else
                IO.Success(deadline)
          } getOrElse IO.Success(deadline)
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
            createdInLevel: Int,
            maxProbe: Int,
            bloomFilterFalsePositiveRate: Double,
            enableRangeFilter: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[(Slice[Byte], Option[Deadline])] =
    if (keyValues.isEmpty)
      IO.Success(Slice.emptyBytes, None)
    else {
      val lastStats = keyValues.last.stats

      val slice = Slice.create[Byte](lastStats.segmentSize)

      val (valuesSlice, sortedIndexSlice, hashIndexSlice, footerHeaderSlice, bloomFilterSlice) =
        slice.splitAt(lastStats.segmentValuesSize) ==> {
          case (valuesSlice, remainingSlice) =>
            remainingSlice.splitAt(lastStats.sortedIndexSize) ==> {
              case (sortedIndexSlice, remainingSlice) =>
                remainingSlice.splitAt(lastStats.segmentHashIndexSize) ==> {
                  case (hashIndexSlice, remainingSlice) =>
                    val footerHeaderSize =
                    //bloomFilter's size it will be written to footer but other will be sliced to use the main slice.
                      lastStats.footerHeaderSize - (lastStats.bloomFilterSize + Bytes.sizeOf(lastStats.rangeFilterSize) + lastStats.rangeFilterSize)

                    remainingSlice.splitAt(footerHeaderSize) ==> {
                      case (footerStartSlice, rangeBloomFooterEndSlice) =>
                        val bloomSlice = rangeBloomFooterEndSlice take lastStats.bloomFilterSize
                        (valuesSlice, sortedIndexSlice, hashIndexSlice, footerStartSlice, bloomSlice)
                    }
                }
            }
        }

      val bloomFilter =
        BloomFilter.init(
          numberOfKeys = lastStats.bloomFilterKeysCount,
          hasRemoveRange = lastStats.hasRemoveRange,
          falsePositiveRate = bloomFilterFalsePositiveRate,
          enableRangeFilter = enableRangeFilter,
          bytes = bloomFilterSlice
        )

      write(
        keyValues = keyValues,
        sortedIndexSlice = sortedIndexSlice,
        valuesSlice = valuesSlice,
        maxProbe = maxProbe,
        bloomFilter = bloomFilter,
        hashIndex =
          Some(
            SegmentHashIndex.WriteResult(
              hit = 0,
              miss = 0,
              maxProbe = maxProbe,
              bytes = hashIndexSlice
            )
          )
      ) flatMap {
        nearestDeadline =>
          IO {
            //this is a placeholder to store the format type of the Segment file written.
            //currently there is only one format. So this is hardcoded but if there are a new file format then
            //SegmentWriter and SegmentReader should be changed to be type classes with unique format types ids.
            //the following group of bytes are also used for CRC check.
            footerHeaderSlice addIntUnsigned SegmentWriter.formatId
            footerHeaderSlice addIntUnsigned createdInLevel
            footerHeaderSlice addBoolean lastStats.hasGroup
            footerHeaderSlice addBoolean lastStats.hasRange
            footerHeaderSlice addBoolean lastStats.hasPut
            footerHeaderSlice addIntUnsigned sortedIndexSlice.fromOffset
            footerHeaderSlice addIntUnsigned hashIndexSlice.fromOffset

            //do CRC
            var indexBytesToCRC = sortedIndexSlice.take(SegmentWriter.crcBytes)
            if (indexBytesToCRC.size < SegmentWriter.crcBytes) //if index does not have enough bytes, fill remaining from the footer.
              indexBytesToCRC = indexBytesToCRC append footerHeaderSlice.take(SegmentWriter.crcBytes - indexBytesToCRC.size)
            assert(indexBytesToCRC.size == SegmentWriter.crcBytes, s"Invalid CRC bytes size: ${indexBytesToCRC.size}. Required: ${SegmentWriter.crcBytes}")
            footerHeaderSlice addLong CRC32.forBytes(indexBytesToCRC)

            //here the top Level key-values are used instead of Group's internal key-values because Group's internal key-values
            //are read when the Group key-value is read.
            footerHeaderSlice addIntUnsigned keyValues.size
            //total number of actual key-values grouped or un-grouped
            footerHeaderSlice addIntUnsigned lastStats.bloomFilterKeysCount

            //write the actual bytes used by bloomFilter.
            val bloomFilterExportSize = bloomFilter.map(_.exportSize).getOrElse(0)
            footerHeaderSlice addInt bloomFilterExportSize //cannot be unsigned int because optimalBloomFilterSize can be larger than actual.
            //footer can also sometimes be not full when calculated size of bloomFilter is expected to be eg: 300 but the resulting size was 20.
            //here a byte will be saved.
            assert(footerHeaderSlice.isFull || footerHeaderSlice.size == footerHeaderSlice.written + 1)

            //move to the last written byte position of bloomFilter on the original slice.
            slice moveWritePositionUnsafe bloomFilter.map(bloom => bloomFilterSlice.fromOffset + bloom.endOffset + 1).getOrElse(bloomFilterSlice.fromOffset)

            //write range filter only if bloomFilter was created.
            bloomFilter foreach {
              filter =>
                slice addIntUnsigned filter.currentRangeFilterBytesRequired
                filter writeRangeFilter slice
            }

            slice addInt footerHeaderSlice.fromOffset

            val actualSegmentSizeWithoutFooter = valuesSlice.size + sortedIndexSlice.size + hashIndexSlice.size

            assert(
              lastStats.segmentSizeWithoutFooter == actualSegmentSizeWithoutFooter,
              s"Invalid segment size. actual: $actualSegmentSizeWithoutFooter - expected: ${lastStats.segmentSizeWithoutFooter}"
            )

            val segmentSlice = Slice(slice.toArray).slice(slice.fromOffset, slice.currentWritePosition - 1)
            (segmentSlice, nearestDeadline)
          }
      }
    }
}
