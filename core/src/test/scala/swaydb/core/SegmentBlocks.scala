///*
// * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//package swaydb.core
//
//import swaydb.core.segment.block.binarysearch.BinarySearchIndexBlock
//import swaydb.core.segment.block.bloomfilter.BloomFilterBlock
//import swaydb.core.segment.block.hashindex.HashIndexBlock
//import swaydb.core.segment.block.reader.UnblockedReader
//import swaydb.core.segment.block.segment.footer.SegmentFooterBlock
//import swaydb.core.segment.block.sortedindex.SortedIndexBlock
//import swaydb.core.segment.block.values.ValuesBlock
//
//case class SegmentBlocks(valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
//                         sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
//                         hashIndexReader: Option[UnblockedReader[HashIndexBlock.Offset, HashIndexBlock]],
//                         binarySearchIndexReader: Option[UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock]],
//                         bloomFilterReader: Option[UnblockedReader[BloomFilterBlock.Offset, BloomFilterBlock]],
//                         footer: SegmentFooterBlock)
