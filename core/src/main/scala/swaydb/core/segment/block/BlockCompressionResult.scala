package swaydb.core.segment.block

import swaydb.data.slice.{Slice, SliceOption}

class BlockCompressionResult(val compressedBytes: SliceOption[Byte],
                             val headerBytes: Slice[Byte])
