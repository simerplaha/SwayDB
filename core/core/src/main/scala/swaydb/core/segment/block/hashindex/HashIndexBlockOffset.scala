package swaydb.core.segment.block.hashindex

import swaydb.core.segment.block.{BlockHeader, BlockOffset, BlockOps}

object HashIndexBlockOffset {

  implicit object HashIndexBlockOps extends BlockOps[HashIndexBlockOffset, HashIndexBlock] {
    override def updateBlockOffset(block: HashIndexBlock, start: Int, size: Int): HashIndexBlock =
      block.copy(offset = createOffset(start = start, size = size))

    override def createOffset(start: Int, size: Int): HashIndexBlockOffset =
      HashIndexBlockOffset(start = start, size = size)

    override def readBlock(header: BlockHeader[HashIndexBlockOffset]): HashIndexBlock =
      HashIndexBlock.read(header)
  }

}

case class HashIndexBlockOffset(start: Int,
                                size: Int) extends BlockOffset
