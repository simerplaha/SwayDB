package swaydb.core

import swaydb.core.io.reader.BlockReader
import swaydb.core.segment.format.a.block.{BinarySearchIndexBlock, BloomFilterBlock, HashIndexBlock, SegmentBlock, SortedIndexBlock, ValuesBlock}


case class Blocks(footer: SegmentBlock.Footer,
                  valuesReader: Option[BlockReader[ValuesBlock]],
                  sortedIndexReader: BlockReader[SortedIndexBlock],
                  hashIndexReader: Option[BlockReader[HashIndexBlock]],
                  binarySearchIndexReader: Option[BlockReader[BinarySearchIndexBlock]],
                  bloomFilterReader: Option[BlockReader[BloomFilterBlock]])