package swaydb.core.segment.block.values

import swaydb.compression.CompressionInternal
import swaydb.core.segment.entry.writer.EntryWriter
import swaydb.data.config.UncompressedBlockInfo
import swaydb.data.slice.Slice

private[segment] class ValuesBlockState(var compressibleBytes: Slice[Byte],
                                        val cacheableBytes: Slice[Byte],
                                        var header: Slice[Byte],
                                        val compressions: UncompressedBlockInfo => Iterable[CompressionInternal],
                                        val builder: EntryWriter.Builder) {

  def blockSize: Int =
    header.size + compressibleBytes.size

  def blockBytes =
    header ++ compressibleBytes
}
