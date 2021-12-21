package swaydb.core.segment.block.segment.footer

import swaydb.slice.SliceMut
import swaydb.utils.SomeOrNone

sealed trait SegmentFooterBlockStateOption extends SomeOrNone[SegmentFooterBlockStateOption, SegmentFooterBlockState] {
  override def noneS: SegmentFooterBlockStateOption =
    SegmentFooterBlockState.Null
}

case object SegmentFooterBlockState {

  final case object Null extends SegmentFooterBlockStateOption {
    override def isNoneS: Boolean = true

    override def getS: SegmentFooterBlockState = throw new Exception(s"${SegmentFooterBlockState.productPrefix} is of type ${Null.productPrefix}")
  }
}


private[block] case class SegmentFooterBlockState(footerSize: Int,
                                                  createdInLevel: Int,
                                                  var bytes: SliceMut[Byte],
                                                  keyValuesCount: Int,
                                                  rangeCount: Int,
                                                  updateCount: Int,
                                                  putCount: Int,
                                                  putDeadlineCount: Int) extends SegmentFooterBlockStateOption {
  override def isNoneS: Boolean =
    false

  override def getS: SegmentFooterBlockState =
    this

}
