package swaydb.core.segment.block

import swaydb.compression.DecompressorInternal

object BlockCompressionInfo {

  @inline def apply(decompressor: DecompressorInternal,
                    decompressedLength: Int): BlockCompressionInfo =
    new BlockCompressionInfo(decompressor, decompressedLength)

}

class BlockCompressionInfo(val decompressor: DecompressorInternal,
                           val decompressedLength: Int)
