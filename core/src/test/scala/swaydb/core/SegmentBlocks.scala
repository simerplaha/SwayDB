/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */
package swaydb.core

import swaydb.core.segment.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.block.bloomfilter.BloomFilterBlock
import swaydb.core.segment.block.hashindex.HashIndexBlock
import swaydb.core.segment.block.reader.UnblockedReader
import swaydb.core.segment.block.segment.footer.SegmentFooterBlock
import swaydb.core.segment.block.sortedindex.SortedIndexBlock
import swaydb.core.segment.block.values.ValuesBlock

case class SegmentBlocks(valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
                         sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                         hashIndexReader: Option[UnblockedReader[HashIndexBlock.Offset, HashIndexBlock]],
                         binarySearchIndexReader: Option[UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock]],
                         bloomFilterReader: Option[UnblockedReader[BloomFilterBlock.Offset, BloomFilterBlock]],
                         footer: SegmentFooterBlock)
