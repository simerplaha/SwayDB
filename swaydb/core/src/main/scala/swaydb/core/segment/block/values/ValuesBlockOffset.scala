package swaydb.core.segment.block.values

import swaydb.core.segment.block.{BlockHeader, BlockOffset, BlockOps}

object ValuesBlockOffset {

  def zero(): ValuesBlockOffset =
    ValuesBlockOffset(0, 0)

  implicit object ValuesBlockOps extends BlockOps[ValuesBlockOffset, ValuesBlock] {
    override def updateBlockOffset(block: ValuesBlock, start: Int, size: Int): ValuesBlock =
      block.copy(offset = createOffset(start = start, size = size))

    override def createOffset(start: Int, size: Int): ValuesBlockOffset =
      ValuesBlockOffset(start = start, size = size)

    override def readBlock(header: BlockHeader[ValuesBlockOffset]): ValuesBlock =
      ValuesBlock.read(header)
  }

}

@inline case class ValuesBlockOffset(start: Int,
                                     size: Int) extends BlockOffset
