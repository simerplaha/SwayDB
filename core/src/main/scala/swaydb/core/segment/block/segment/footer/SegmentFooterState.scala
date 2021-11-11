package swaydb.core.segment.block.segment.footer

import swaydb.data.slice.Slice

private[block] case class SegmentFooterState(footerSize: Int,
                                             createdInLevel: Int,
                                             var bytes: Slice[Byte],
                                             keyValuesCount: Int,
                                             rangeCount: Int,
                                             updateCount: Int,
                                             putCount: Int,
                                             putDeadlineCount: Int)
