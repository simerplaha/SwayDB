package swaydb.core.segment.block

import swaydb.data.slice.{SliceMut, SliceOption}

class BlockCompressionResult(val compressedBytes: SliceOption[Byte],
                             val headerBytes: SliceMut[Byte])
