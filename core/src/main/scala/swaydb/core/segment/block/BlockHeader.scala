package swaydb.core.segment.block

import swaydb.data.slice.ReaderBase

class BlockHeader[O](val compressionInfo: Option[BlockCompressionInfo],
                     val headerReader: ReaderBase[Byte],
                     val headerSize: Int,
                     val offset: O)
