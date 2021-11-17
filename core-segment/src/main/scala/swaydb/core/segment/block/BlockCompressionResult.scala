package swaydb.core.segment.block

import swaydb.slice.{SliceMut, SliceOption}

class BlockCompressionResult(val compressedBytes: SliceOption[Byte],
                             val headerBytes: SliceMut[Byte])
