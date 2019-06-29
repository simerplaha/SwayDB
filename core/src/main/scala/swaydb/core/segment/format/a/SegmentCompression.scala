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

import swaydb.compression.CompressionInternal

object SegmentCompression {
  def apply(bloomFilter: swaydb.data.config.BloomFilter,
            hashIndex: swaydb.data.config.HashIndex,
            binarySearchIndex: swaydb.data.config.BinarySearchIndex,
            sortedIndex: swaydb.data.config.SortedIndex,
            values: swaydb.data.config.Values): SegmentCompression =
    SegmentCompression(
      values = bloomFilter.toOption.map(_.compression.map(CompressionInternal.apply)).getOrElse(Seq.empty),
      sortedIndex = sortedIndex.toOption.map(_.compression.map(CompressionInternal.apply)).getOrElse(Seq.empty),
      hashIndex = hashIndex.toOption.map(_.compression.map(CompressionInternal.apply)).getOrElse(Seq.empty),
      binarySearchIndex = binarySearchIndex.toOption.map(_.compression.map(CompressionInternal.apply)).getOrElse(Seq.empty),
      bloomFilter = bloomFilter.toOption.map(_.compression.map(CompressionInternal.apply)).getOrElse(Seq.empty)
    )
}

case class SegmentCompression(values: Seq[CompressionInternal],
                              sortedIndex: Seq[CompressionInternal],
                              hashIndex: Seq[CompressionInternal],
                              binarySearchIndex: Seq[CompressionInternal],
                              bloomFilter: Seq[CompressionInternal])