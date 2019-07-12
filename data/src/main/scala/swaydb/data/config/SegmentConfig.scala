package swaydb.data.config

import swaydb.data.api.grouping.Compression

case class SegmentConfig(blockIO: BlockInfo => BlockIO,
                         compression: Seq[Compression])