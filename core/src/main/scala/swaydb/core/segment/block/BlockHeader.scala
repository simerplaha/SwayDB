package swaydb.core.segment.block

import swaydb.slice.SliceReader

object BlockHeader {
  val uncompressedBlockId: Byte = 0.toByte
  val compressedBlockID: Byte = 1.toByte
}

class BlockHeader[O](val compressionInfo: BlockCompressionInfoOption,
                     val headerReader: SliceReader,
                     val headerSize: Int,
                     val offset: O)
