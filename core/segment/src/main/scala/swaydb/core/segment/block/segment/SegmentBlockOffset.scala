package swaydb.core.segment.block.segment

import swaydb.core.segment.block.{BlockHeader, BlockOffset, BlockOps}

object SegmentBlockOffset {

  def empty =
    SegmentBlockOffset(0, 0)

  implicit object SegmentBlockOps extends BlockOps[SegmentBlockOffset, SegmentBlock] {
    override def updateBlockOffset(block: SegmentBlock, start: Int, size: Int): SegmentBlock =
      block.copy(offset = SegmentBlockOffset(start = start, size = size))

    override def createOffset(start: Int, size: Int): SegmentBlockOffset =
      SegmentBlockOffset(start, size)

    override def readBlock(header: BlockHeader[SegmentBlockOffset]): SegmentBlock =
      SegmentBlock.read(header)
  }

}

case class SegmentBlockOffset(start: Int,
                              size: Int) extends BlockOffset
