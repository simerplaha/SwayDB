package swaydb.core.segment.block.binarysearch

import swaydb.core.segment.block.BlockOffset

case class BinarySearchIndexBlockOffset(start: Int,
                                        size: Int) extends BlockOffset
