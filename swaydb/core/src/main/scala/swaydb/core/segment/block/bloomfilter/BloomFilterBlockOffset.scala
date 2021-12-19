package swaydb.core.segment.block.bloomfilter

import swaydb.core.segment.block.{BlockHeader, BlockOffset, BlockOps}

object BloomFilterBlockOffset {
  implicit object BloomFilterBlockOps extends BlockOps[BloomFilterBlockOffset, BloomFilterBlock] {
    override def updateBlockOffset(block: BloomFilterBlock, start: Int, size: Int): BloomFilterBlock =
      block.copy(offset = createOffset(start = start, size = size))

    override def createOffset(start: Int, size: Int): BloomFilterBlockOffset =
      BloomFilterBlockOffset(start = start, size = size)

    override def readBlock(header: BlockHeader[BloomFilterBlockOffset]): BloomFilterBlock =
      BloomFilterBlock.read(header)
  }
}

@inline case class BloomFilterBlockOffset(start: Int,
                                          size: Int) extends BlockOffset
