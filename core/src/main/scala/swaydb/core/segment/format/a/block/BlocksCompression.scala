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

package swaydb.core.segment.format.a.block

import swaydb.compression.CompressionInternal
import swaydb.data.api.grouping.Compression

object BlocksCompression {

  def disabled =
    BlocksCompression(
      values = Seq.empty,
      sortedIndex = Seq.empty,
      hashIndex = Seq.empty,
      binarySearchIndex = Seq.empty,
      bloomFilter = Seq.empty,
      segmentCompression = Seq.empty
    )

  def apply(bloomFilter: swaydb.data.config.MightContainKeyIndex,
            hashIndex: swaydb.data.config.RandomKeyIndex,
            binarySearchIndex: swaydb.data.config.BinarySearchKeyIndex,
            sortedIndex: swaydb.data.config.SortedKeyIndex,
            values: swaydb.data.config.ValuesConfig,
            segment: Seq[Compression]): BlocksCompression =
    BlocksCompression(
      values = values.toOption.map(_.compression.map(CompressionInternal.apply)).getOrElse(Seq.empty),
      sortedIndex = sortedIndex.toOption.map(_.compression.map(CompressionInternal.apply)).getOrElse(Seq.empty),
      hashIndex = hashIndex.toOption.map(_.compression.map(CompressionInternal.apply)).getOrElse(Seq.empty),
      binarySearchIndex = binarySearchIndex.toOption.map(_.compression.map(CompressionInternal.apply)).getOrElse(Seq.empty),
      bloomFilter = bloomFilter.toOption.map(_.compression.map(CompressionInternal.apply)).getOrElse(Seq.empty),
      segmentCompression = segment map CompressionInternal.apply
    )
}

case class BlocksCompression(values: Seq[CompressionInternal],
                             sortedIndex: Seq[CompressionInternal],
                             hashIndex: Seq[CompressionInternal],
                             binarySearchIndex: Seq[CompressionInternal],
                             bloomFilter: Seq[CompressionInternal],
                             segmentCompression: Seq[CompressionInternal])