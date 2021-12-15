package swaydb.core.segment.block.bloomfilter

import swaydb.core.compression.CoreCompression
import swaydb.config.UncompressedBlockInfo
import swaydb.slice.{Slice, SliceMut}

private[core] class BloomFilterBlockState(val numberOfBits: Int,
                                          val maxProbe: Int,
                                          var compressibleBytes: SliceMut[Byte],
                                          val cacheableBytes: Slice[Byte],
                                          var header: Slice[Byte],
                                          val compressions: UncompressedBlockInfo => Iterable[CoreCompression]) {

  def blockSize: Int =
    header.size + compressibleBytes.size

  def blockBytes: Slice[Byte] =
    header ++ compressibleBytes

  def written: Int =
    compressibleBytes.size

  override def hashCode(): Int =
    compressibleBytes.hashCode()
}
