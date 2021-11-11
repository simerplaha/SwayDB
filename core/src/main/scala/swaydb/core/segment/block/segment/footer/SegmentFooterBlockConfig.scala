package swaydb.core.segment.block.segment.footer

import swaydb.effect.{IOAction, IOStrategy}

object SegmentFooterBlockConfig {

  def default() =
    SegmentFooterBlockConfig(
      blockIO = IOStrategy.defaultSynchronised
    )

}

case class SegmentFooterBlockConfig(blockIO: IOAction => IOStrategy)
