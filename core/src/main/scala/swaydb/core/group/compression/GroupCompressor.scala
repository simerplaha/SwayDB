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
import swaydb.core.data.{KeyValue, Transient}
import swaydb.core.group.compression.GroupCompressorFailure.InvalidGroupKeyValuesHeadPosition
import swaydb.core.segment.format.a.block._
import swaydb.core.segment.format.a.{SegmentCompression, SegmentWriter}
import swaydb.data.slice.Slice
import swaydb.data.{IO, MaxKey}

private[core] object GroupCompressor extends LazyLogging {

  /**
    * Returns (fromKey, toKey, fullKey) where fullKey is compressed fromKey and toKey.
    *
    * Pre-requisite: keyValues should be non-empty.
    */
  def buildCompressedKey(keyValues: Iterable[KeyValue.WriteOnly]): (Slice[Byte], MaxKey[Slice[Byte]], Slice[Byte]) =
    GroupKeyCompressor.compress(keyValues.headOption, keyValues.last)

  def compress(keyValues: Slice[KeyValue.WriteOnly],
               previous: Option[KeyValue.WriteOnly],
               segmentCompression: SegmentCompression,
               valuesConfig: Values.Config,
               sortedIndexConfig: SortedIndex.Config,
               binarySearchIndexConfig: BinarySearchIndex.Config,
               hashIndexConfig: HashIndex.Config,
               bloomFilterConfig: BloomFilter.Config): IO[Option[Transient.Group]] =
    if (keyValues.isEmpty) {
      logger.error(s"Ignoring compression. Cannot compress on empty key-values")
      IO.none
    } else if (keyValues.head.stats.chainPosition != 1) {
      //Cannot write key-values that belong to another Group or Segment. Groups key-values should have stats reset.
      val message = InvalidGroupKeyValuesHeadPosition(keyValues.head.stats.chainPosition)
      logger.error(message.getMessage)
      IO.Failure(message)
    } else {
      logger.debug(s"Compressing ${keyValues.size} key-values with previous key-value as ${previous.map(_.getClass.getSimpleName)}.")
      SegmentWriter.write(
        keyValues = keyValues,
        segmentCompression = segmentCompression,
        createdInLevel = 0,
        maxProbe = keyValues.last.hashIndexConfig.maxProbe
      ) flatMap {
        result =>
          IO {
            val (minKey, maxKey, fullKey) = buildCompressedKey(keyValues)
            Some(
              Transient.Group(
                minKey = minKey,
                maxKey = maxKey,
                fullKey = fullKey,
                result = result,
                deadline = result.nearestDeadline,
                keyValues = keyValues,
                previous = previous,
                valuesConfig = valuesConfig,
                sortedIndexConfig = sortedIndexConfig,
                binarySearchIndexConfig = binarySearchIndexConfig,
                hashIndexConfig = hashIndexConfig,
                bloomFilterConfig = bloomFilterConfig
              )
            )
          }
      }
    }
}
