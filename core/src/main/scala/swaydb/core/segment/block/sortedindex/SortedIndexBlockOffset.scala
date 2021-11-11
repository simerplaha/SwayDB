package swaydb.core.segment.block.sortedindex

import swaydb.core.segment.block.{BlockHeader, BlockOffset, BlockOps}

object SortedIndexBlockOffset {

  implicit object SortedIndexBlockOps extends BlockOps[SortedIndexBlockOffset, SortedIndexBlock] {
    override def updateBlockOffset(block: SortedIndexBlock, start: Int, size: Int): SortedIndexBlock =
      block.copy(offset = createOffset(start = start, size = size))

    override def createOffset(start: Int, size: Int): SortedIndexBlockOffset =
      SortedIndexBlockOffset(start = start, size = size)

    override def readBlock(header: BlockHeader[SortedIndexBlockOffset]): SortedIndexBlock =
      SortedIndexBlock.read(header)
  }

}

case class SortedIndexBlockOffset(start: Int,
                                  size: Int) extends BlockOffset
