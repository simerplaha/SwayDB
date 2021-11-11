package swaydb.core.segment.block.segment.footer

import swaydb.effect.{IOAction, IOStrategy}

object SegmentFooterConfig {

  def default() =
    SegmentFooterConfig(
      blockIO = IOStrategy.defaultSynchronised
    )

}

case class SegmentFooterConfig(blockIO: IOAction => IOStrategy)
