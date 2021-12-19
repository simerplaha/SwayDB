package swaydb.core.segment.block.binarysearch

import swaydb.core.segment.block.{BlockHeader, BlockOffset, BlockOps}

object BinarySearchIndexBlockOffset {

  implicit object BinarySearchIndexBlockOps extends BlockOps[BinarySearchIndexBlockOffset, BinarySearchIndexBlock] {
    override def updateBlockOffset(block: BinarySearchIndexBlock, start: Int, size: Int): BinarySearchIndexBlock =
      block.copy(offset = BinarySearchIndexBlockOffset(start = start, size = size))

    override def createOffset(start: Int, size: Int): BinarySearchIndexBlockOffset =
      BinarySearchIndexBlockOffset(start, size)

    override def readBlock(header: BlockHeader[BinarySearchIndexBlockOffset]): BinarySearchIndexBlock =
      BinarySearchIndexBlock.read(header)
  }

}

@inline case class BinarySearchIndexBlockOffset(start: Int,
                                                size: Int) extends BlockOffset
