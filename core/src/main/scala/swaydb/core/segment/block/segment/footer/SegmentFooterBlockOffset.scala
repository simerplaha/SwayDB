package swaydb.core.segment.block.segment.footer

import swaydb.core.segment.block.{BlockHeader, BlockOffset, BlockOps}

object SegmentFooterBlockOffset {

  implicit object SegmentFooterBlockOps extends BlockOps[SegmentFooterBlockOffset, SegmentFooterBlock] {
    override def updateBlockOffset(block: SegmentFooterBlock, start: Int, size: Int): SegmentFooterBlock =
      block.copy(offset = createOffset(start, size))

    override def createOffset(start: Int, size: Int): SegmentFooterBlockOffset =
      SegmentFooterBlockOffset(start, size)

    override def readBlock(header: BlockHeader[SegmentFooterBlockOffset]): SegmentFooterBlock =
      throw new Exception("Footers do not have block header readers.")
  }

}

case class SegmentFooterBlockOffset(start: Int,
                                    size: Int) extends BlockOffset
