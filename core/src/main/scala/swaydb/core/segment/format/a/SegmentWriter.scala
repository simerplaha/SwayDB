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
import swaydb.core.data.{KeyValue, Transient}
import swaydb.core.segment.Segment
import swaydb.core.segment.format.a.index.{BinarySearchIndex, BloomFilter, HashIndex}
import swaydb.core.util.CRC32
import swaydb.data.IO
import swaydb.data.IO._
import swaydb.data.slice.Slice
import swaydb.data.slice.Slice._

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Deadline

private[core] object SegmentWriter extends LazyLogging {

  val formatId: Byte = 1.toByte

  val crcBytes: Int = 7

  object Result {
    val empty =
      Result(
        values = None,
        sortedIndex = Slice.emptyBytes,
        hashIndex = None,
        binarySearchIndex = None,
        bloomFilter = None,
        footer = Slice.emptyBytes,
        nearestDeadline = None
      )

    val emptyIO = IO.Success(empty)
  }

  case class Result(values: Option[Slice[Byte]],
                    sortedIndex: Slice[Byte],
                    hashIndex: Option[Slice[Byte]],
                    binarySearchIndex: Option[Slice[Byte]],
                    bloomFilter: Option[Slice[Byte]],
                    footer: Slice[Byte],
                    nearestDeadline: Option[Deadline]) {

    val segmentBytes: Seq[Slice[Byte]] = {
      val all = ListBuffer.empty[Slice[Byte]]
      values foreach (all += _)
      all += sortedIndex
      hashIndex foreach (all += _)
      binarySearchIndex foreach (all += _)
      bloomFilter foreach (all += _)
      all += footer
      all
    }

    def isEmpty =
      sortedIndex.isEmpty || footer.isEmpty

    def segmentSize =
      segmentBytes.foldLeft(0)(_ + _.written)

    def flattenBytes: Slice[Byte] = {
      val size = segmentBytes.foldLeft(0)(_ + _.written)
      val slice = Slice.create[Byte](size)
      segmentBytes.map(_.unslice()) foreach slice.addAll
      assert(slice.isFull)
      slice
    }

    def flatten: (Slice[Byte], Option[Deadline]) =
      (flattenBytes, nearestDeadline)
  }

  def writeIndexes(keyValue: KeyValue.WriteOnly,
                   hashIndex: Option[HashIndex.State],
                   binarySearchIndex: Option[BinarySearchIndex.State],
                   bloomFilter: Option[BloomFilter.State],
                   currentNearestDeadline: Option[Deadline]): IO[Option[Deadline]] = {

    def writeOne(rootGroup: Option[KeyValue.WriteOnly.Group],
                 keyValue: KeyValue.WriteOnly): IO[Unit] =
      keyValue match {
        case childGroup: KeyValue.WriteOnly.Group =>
          writeMany(
            rootGroup = rootGroup,
            keyValues = childGroup.keyValues
          )

        case keyValue @ (_: KeyValue.WriteOnly.Range | _: KeyValue.WriteOnly.Fixed) =>
          val thisKeyValuesAccessOffset =
            rootGroup
              .map(_.stats.thisKeyValuesAccessIndexOffset)
              .getOrElse(keyValue.stats.thisKeyValuesAccessIndexOffset)

          bloomFilter foreach (BloomFilter.add(keyValue.key, _))

          hashIndex map {
            hashIndexState =>
              HashIndex.write(
                key = keyValue.key,
                value = thisKeyValuesAccessOffset,
                state = hashIndexState
              )
          } match {
            case Some(IO.Success(hit)) if hit && binarySearchIndex.forall(!_.buildFullBinarySearchIndex) =>
              IO.unit

            case None | Some(IO.Success(_)) =>
              binarySearchIndex map {
                state =>
                  BinarySearchIndex.write(
                    value = thisKeyValuesAccessOffset,
                    state = state
                  )
              } getOrElse IO.unit

            case Some(IO.Failure(error)) =>
              IO.Failure(error)
          }
      }

    @tailrec
    def writeMany(rootGroup: Option[KeyValue.WriteOnly.Group],
                  keyValues: Slice[KeyValue.WriteOnly]): IO[Unit] =
      keyValues.headOption match {
        case Some(keyValue) =>
          writeOne(rootGroup, keyValue)
          writeMany(rootGroup, keyValues.drop(1))

        case None =>
          IO.unit
      }

    @tailrec
    def write(keyValues: Slice[KeyValue.WriteOnly], nearestDeadline: Option[Deadline]): Option[Deadline] =
      keyValues.headOption match {
        case Some(keyValue) =>
          val nextNearestDeadline = Segment.getNearestDeadline(nearestDeadline, keyValue)

          if (hashIndex.isDefined || binarySearchIndex.isDefined || bloomFilter.isDefined)
            keyValue match {
              case rootGroup: KeyValue.WriteOnly.Group =>
                writeMany(
                  rootGroup = rootGroup.asInstanceOf[Some[Transient.Group]],
                  keyValues = rootGroup.keyValues
                ).get

              case _: KeyValue.WriteOnly.Range | _: KeyValue.WriteOnly.Fixed =>
                writeOne(
                  rootGroup = None,
                  keyValue = keyValue
                ).get
            }

          write(keyValues.drop(1), nextNearestDeadline)

        case None =>
          nearestDeadline
      }

    IO {
      write(
        keyValues = Slice(keyValue),
        nearestDeadline = currentNearestDeadline
      )
    }
  }

  private def writeHeaders(hashIndex: Option[HashIndex.State],
                           bloomFilter: Option[BloomFilter.State],
                           binarySearchIndex: Option[BinarySearchIndex.State]): IO[Unit] =
    IO {
      hashIndex foreach HashIndex.writeHeader
      binarySearchIndex foreach BinarySearchIndex.writeHeader
    }

  private def writeKeyValueBytes(keyValue: KeyValue.WriteOnly,
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
                                hashIndex: Option[HashIndex.State],
                                bloomFilter: Option[BloomFilter.State],
                                binarySearchIndex: Option[BinarySearchIndex.State],
                                deadline: Option[Deadline]): IO[Option[Deadline]] =
    writeKeyValueBytes(
      keyValue = keyValue,
      sortedIndexSlice = sortedIndexSlice,
      valuesSlice = valuesSlice
    ) flatMap {
      _ =>
        writeIndexes(
          keyValue = keyValue,
          hashIndex = hashIndex,
          bloomFilter = bloomFilter,
          binarySearchIndex = binarySearchIndex,
          currentNearestDeadline = deadline
        ) flatMap {
          nearestDeadline =>
            writeHeaders(
              hashIndex = hashIndex,
              bloomFilter = bloomFilter,
              binarySearchIndex = binarySearchIndex
            ) map {
              _ =>
                nearestDeadline
            }
        }
    }

  def write(keyValues: Iterable[KeyValue.WriteOnly],
            sortedIndexSlice: Slice[Byte],
            valuesSlice: Slice[Byte],
            hashIndex: Option[HashIndex.State],
            binarySearchIndex: Option[BinarySearchIndex.State],
            bloomFilter: Option[BloomFilter.State]): IO[Option[Deadline]] =
    keyValues.foldLeftIO(Option.empty[Deadline]) {
      case (deadline, keyValue) =>
        writeSegmentBytes(
          keyValue = keyValue,
          sortedIndexSlice = sortedIndexSlice,
          valuesSlice = valuesSlice,
          hashIndex = hashIndex,
          bloomFilter = bloomFilter,
          binarySearchIndex = binarySearchIndex,
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
          IO.Success(deadline)
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
            maxProbe: Int): IO[Result] =
    if (keyValues.isEmpty)
      Result.emptyIO
    else {
      val lastStats = keyValues.last.stats

      val hashIndex =
        HashIndex.init(
          maxProbe = maxProbe,
          size = lastStats.segmentHashIndexSize
        )

      val binarySearchIndex =
        if (lastStats.binarySearchIndexSize <= 1)
          None
        else
          Some(
            BinarySearchIndex.State(
              largestValue = lastStats.thisKeyValuesAccessIndexOffset,
              valuesCount = lastStats.segmentUniqueKeysCount,
              buildFullBinarySearchIndex = keyValues.last.buildFullBinarySearchIndex,
              bytes = Slice.create[Byte](lastStats.binarySearchIndexSize)
            )
          )

      val bloomFilter = BloomFilter.init(keyValues)

      val sortedIndexSlice = Slice.create[Byte](lastStats.segmentSortedIndexSize)
      val valuesSlice = Slice.create[Byte](lastStats.segmentValuesSize)

      write(
        keyValues = keyValues,
        sortedIndexSlice = sortedIndexSlice,
        valuesSlice = valuesSlice,
        hashIndex = hashIndex,
        binarySearchIndex = binarySearchIndex,
        bloomFilter = bloomFilter
      ) flatMap {
        nearestDeadline =>
          IO {
            val segmentFooterSlice = Slice.create[Byte](lastStats.segmentFooterSize)
            //this is a placeholder to store the format type of the Segment file written.
            //currently there is only one format. So this is hardcoded but if there are a new file format then
            //SegmentWriter and SegmentReader should be changed to be type classes with unique format types ids.
            //the following group of bytes are also used for CRC check.
            segmentFooterSlice addIntUnsigned SegmentWriter.formatId
            segmentFooterSlice addIntUnsigned createdInLevel
            segmentFooterSlice addBoolean lastStats.segmentHasGroup
            segmentFooterSlice addBoolean lastStats.segmentHasRange
            segmentFooterSlice addBoolean lastStats.segmentHasPut
            //here the top Level key-values are used instead of Group's internal key-values because Group's internal key-values
            //are read when the Group key-value is read.
            segmentFooterSlice addIntUnsigned keyValues.size
            //total number of actual key-values grouped or un-grouped
            segmentFooterSlice addIntUnsigned lastStats.segmentUniqueKeysCount

            //do CRC
            val indexBytesToCRC = segmentFooterSlice.take(SegmentWriter.crcBytes)
            assert(indexBytesToCRC.size == SegmentWriter.crcBytes, s"Invalid CRC bytes size: ${indexBytesToCRC.size}. Required: ${SegmentWriter.crcBytes}")
            segmentFooterSlice addLong CRC32.forBytes(indexBytesToCRC)

            var segmentOffset = valuesSlice.written

            segmentFooterSlice addIntUnsigned sortedIndexSlice.written
            segmentFooterSlice addIntUnsigned segmentOffset
            segmentOffset = segmentOffset + sortedIndexSlice.written

            hashIndex map {
              hashIndex =>
                segmentFooterSlice addIntUnsigned hashIndex.bytes.written
                segmentFooterSlice addIntUnsigned segmentOffset
                segmentOffset = segmentOffset + hashIndex.bytes.written
            } getOrElse {
              segmentFooterSlice addIntUnsigned 0
              segmentOffset = segmentOffset + 1
            }

            binarySearchIndex map {
              binarySearchIndex =>
                segmentFooterSlice addIntUnsigned binarySearchIndex.bytes.written
                segmentFooterSlice addIntUnsigned segmentOffset
                segmentOffset = segmentOffset + binarySearchIndex.bytes.written
            } getOrElse {
              segmentFooterSlice addIntUnsigned 0
              segmentOffset = segmentOffset + 1
            }

            bloomFilter map {
              bloomFilter =>
                segmentFooterSlice addIntUnsigned bloomFilter.bytes.written
                segmentFooterSlice addIntUnsigned segmentOffset
                segmentOffset = segmentOffset + bloomFilter.bytes.written
            } getOrElse {
              segmentFooterSlice addIntUnsigned 0
              segmentOffset = segmentOffset + 1
            }

            segmentFooterSlice addInt segmentOffset

            Result(
              values = if (valuesSlice.isEmpty) None else Some(valuesSlice),
              sortedIndex = sortedIndexSlice,
              hashIndex = hashIndex.map(_.bytes),
              binarySearchIndex = binarySearchIndex.map(_.bytes),
              bloomFilter = bloomFilter.map(_.bytes),
              footer = segmentFooterSlice,
              nearestDeadline = nearestDeadline
            )
          }
      }
    }
}
