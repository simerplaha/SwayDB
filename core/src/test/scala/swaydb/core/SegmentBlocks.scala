/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package swaydb.core

import swaydb.core.segment.block.binarysearch.{BinarySearchIndexBlock, BinarySearchIndexBlockOffset}
import swaydb.core.segment.block.bloomfilter.{BloomFilterBlock, BloomFilterBlockOffset}
import swaydb.core.segment.block.hashindex.{HashIndexBlock, HashIndexBlockOffset}
import swaydb.core.segment.block.reader.UnblockedReader
import swaydb.core.segment.block.segment.footer.SegmentFooterBlock
import swaydb.core.segment.block.sortedindex.{SortedIndexBlock, SortedIndexBlockOffset}
import swaydb.core.segment.block.values.{ValuesBlock, ValuesBlockOffset}

case class SegmentBlocks(valuesReader: Option[UnblockedReader[ValuesBlockOffset, ValuesBlock]],
                         sortedIndexReader: UnblockedReader[SortedIndexBlockOffset, SortedIndexBlock],
                         hashIndexReader: Option[UnblockedReader[HashIndexBlockOffset, HashIndexBlock]],
                         binarySearchIndexReader: Option[UnblockedReader[BinarySearchIndexBlockOffset, BinarySearchIndexBlock]],
                         bloomFilterReader: Option[UnblockedReader[BloomFilterBlockOffset, BloomFilterBlock]],
                         footer: SegmentFooterBlock)
