package swaydb.core.segment.block.segment.footer

import swaydb.data.slice.SliceMut

private[block] case class SegmentFooterBlockState(footerSize: Int,
                                                  createdInLevel: Int,
                                                  var bytes: SliceMut[Byte],
                                                  keyValuesCount: Int,
                                                  rangeCount: Int,
                                                  updateCount: Int,
                                                  putCount: Int,
                                                  putDeadlineCount: Int)
