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

package swaydb.core.group.compression

import com.typesafe.scalalogging.LazyLogging
import swaydb.compression.CompressionInternal
import swaydb.core.data.Compressor.{GroupCompressionResult, ValueCompressionResult}
import swaydb.core.data.{Compressor, KeyValue, Transient}
import swaydb.core.group.compression.GroupCompressorFailure.InvalidGroupKeyValuesHeadPosition
import swaydb.core.segment.format.a.{SegmentHashIndex, SegmentWriter}
import swaydb.core.util.Bytes
import swaydb.data.slice.Slice
import swaydb.data.{IO, MaxKey}

private[core] object GroupCompressor extends LazyLogging {

  //currently there is one format. This formatId is a placeholder to support future updates.
  val formatId = 0

  /**
    * Returns (fromKey, toKey, fullKey) where fullKey is compressed fromKey and toKey.
    *
    * Pre-requisite: keyValues should be non-empty.
    */
  def buildCompressedKey(keyValues: Iterable[KeyValue.WriteOnly]): (Slice[Byte], MaxKey[Slice[Byte]], Slice[Byte]) =
    GroupKeyCompressor.compress(keyValues.headOption, keyValues.last)

  def compress(keyValues: Slice[KeyValue.WriteOnly],
               indexCompressions: Seq[CompressionInternal],
               valueCompressions: Seq[CompressionInternal],
               falsePositiveRate: Double,
               resetPrefixCompressionEvery: Int,
               minimumNumberOfKeyForHashIndex: Int,
               hashIndexCompensation: Int => Int,
               previous: Option[KeyValue.WriteOnly],
               maxProbe: Int): IO[Option[Transient.Group]] =
    if (keyValues.isEmpty) {
      logger.error(s"Ignoring compression. Cannot compress on empty key-values")
      IO.none
    } else if (keyValues.head.stats.position != 1) {
      //Cannot write key-values that belong to another Group or Segment. Groups key-values should have stats reset.
      val message = InvalidGroupKeyValuesHeadPosition(keyValues.head.stats.position)
      logger.error(message.getMessage)
      IO.Failure(message)
    } else {
      logger.debug(s"Compressing ${keyValues.size} key-values with previous key-value as ${previous.map(_.getClass.getSimpleName)}.")
      val lastStats = keyValues.last.stats

      val indexBytesRequired = lastStats.sortedIndexSize
      val valueBytesRequired = lastStats.segmentValuesSize
      val hashIndexBytesRequired = lastStats.segmentHashIndexSize

      //write raw key-values bytes.
      val indexBytes = Slice.create[Byte](indexBytesRequired)
      val valueBytes = Slice.create[Byte](valueBytesRequired)
      val hashIndexBytes = Slice.create[Byte](hashIndexBytesRequired)

      SegmentWriter.write(
        keyValues = keyValues,
        sortedIndexSlice = indexBytes,
        valuesSlice = valueBytes,
        bloomFilter = None,
        maxProbe = maxProbe,
        hashIndex =
          Some(
            SegmentHashIndex.State(
              hit = 0,
              miss = 0,
              maxProbe = maxProbe,
              bytes = hashIndexBytes,
              commonRangePrefixesCount = lastStats.rangeCommonPrefixesCount
            )
          )
      ) flatMap {
        nearestDeadline =>
          assert(hashIndexBytes.isFull)

          //compress key-value bytes and write to group with meta data for key bytes and value bytes.
          Compressor.compress(
            indexBytes = indexBytes,
            indexCompressions = indexCompressions,
            valueBytes = valueBytes,
            valueCompressions = valueCompressions,
            keyValueCount = keyValues.size
          ) flatMap {
            case Some(GroupCompressionResult(compressedIndexBytes, indexCompression, valuesCompressionResult)) =>
              //calculate the total size of bytes required including the compressed keys and values byte sizes.
              val headerSize =
                Bytes.sizeOf(formatId) + //format id
                  1 + //for hasRange
                  1 + //for hasPut
                  Bytes.sizeOf(indexCompression.decompressor.id) + //index compression id
                  //key-value count. Use the stats position because in the future a Group might also be compressed with other groups.
                  Bytes.sizeOf(keyValues.last.stats.position) +
                  Bytes.sizeOf(keyValues.last.stats.totalBloomFiltersItemsCount) +
                  Bytes.sizeOf(indexBytesRequired) + //index de-compressed size
                  Bytes.sizeOf(compressedIndexBytes.size) + //index compressed size. These bytes get added to the end.
                  Bytes.sizeOf(hashIndexBytes.size) + {
                  valuesCompressionResult map {
                    case ValueCompressionResult(compressedValuesBytes, valuesCompression) =>
                      Bytes.sizeOf(valuesCompression.decompressor.id) + //values id
                        Bytes.sizeOf(valueBytesRequired) + //values de-compressed size
                        Bytes.sizeOf(compressedValuesBytes.size) // values compressed size
                  } getOrElse 0
                }

              val totalCompressedKeyValueBytesRequired =
                Bytes.sizeOf(headerSize) +
                  headerSize +
                  compressedIndexBytes.size +
                  hashIndexBytes.size +
                  valuesCompressionResult.map(_.compressedValues.size).getOrElse(0)

              //also add header size
              val compressedKeyValueBytes = Slice.create[Byte](totalCompressedKeyValueBytesRequired)

              compressedKeyValueBytes
                .addIntUnsigned(headerSize) //write header size
                .addIntUnsigned(formatId) //format
                .addBoolean(keyValues.last.stats.hasRange)
                .addBoolean(keyValues.last.stats.hasPut)
                .addIntUnsigned(indexCompression.decompressor.id)
                .addIntUnsigned(keyValues.last.stats.position)
                .addIntUnsigned(keyValues.last.stats.totalBloomFiltersItemsCount)
                .addIntUnsigned(indexBytesRequired)
                .addIntUnsigned(compressedIndexBytes.size)
                .addIntUnsigned(hashIndexBytes.size)

              valuesCompressionResult map {
                case ValueCompressionResult(compressedValueBytes, valuesCompression) =>
                  compressedKeyValueBytes
                    .addIntUnsigned(valuesCompression.decompressor.id)
                    .addIntUnsigned(valueBytesRequired)
                    .addIntUnsigned(compressedValueBytes.size)
                    .addAll(compressedValueBytes)
              }

              compressedKeyValueBytes addAll compressedIndexBytes
              //hash indexing alters the position of the bytes which is screwing things up.
              //the write position needs to be reset so that iterators work.
              //set the write position to toOffset +1. (hashIndexBytes.size -1) will not work because
              //for the iterators to complete writePosition should be pointing to the next offset. It's a tricky.
              //moveWritePositionUnsafe needs to be safe.
              hashIndexBytes moveWritePositionUnsafe hashIndexBytes.size
              compressedKeyValueBytes addAll hashIndexBytes

              if (!compressedKeyValueBytes.isFull)
                IO.Failure(new IllegalArgumentException(s"compressedKeyValueBytes Slice is not full. actual: ${compressedKeyValueBytes.written}, expected: ${compressedKeyValueBytes.size}"))
              else
                IO {
                  val (minKey, maxKey, fullKey) = buildCompressedKey(keyValues)
                  Some(
                    Transient.Group(
                      minKey = minKey,
                      maxKey = maxKey,
                      fullKey = fullKey,
                      compressedKeyValues = compressedKeyValueBytes,
                      deadline = nearestDeadline,
                      keyValues = keyValues,
                      previous = previous,
                      falsePositiveRate = falsePositiveRate,
                      resetPrefixCompressionEvery = resetPrefixCompressionEvery,
                      minimumNumberOfKeysForHashIndex = minimumNumberOfKeyForHashIndex,
                      hashIndexCompensation = hashIndexCompensation
                    )
                  )
                }

            case None =>
              IO.none
          }
      }
    }
}
