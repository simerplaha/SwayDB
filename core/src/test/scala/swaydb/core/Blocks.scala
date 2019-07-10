package swaydb.core

import swaydb.core.io.reader.BlockReader
import swaydb.core.segment.format.a.block.{BinarySearchIndex, BloomFilter, HashIndex, SegmentBlock, SortedIndex, Values}


case class Blocks(footer: SegmentBlock.Footer,
                  valuesReader: Option[BlockReader[Values]],
                  sortedIndexReader: BlockReader[SortedIndex],
                  hashIndexReader: Option[BlockReader[HashIndex]],
                  binarySearchIndexReader: Option[BlockReader[BinarySearchIndex]],
                  bloomFilterReader: Option[BlockReader[BloomFilter]])