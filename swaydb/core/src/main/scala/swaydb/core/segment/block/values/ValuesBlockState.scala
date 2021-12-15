package swaydb.core.segment.block.values

import swaydb.core.compression.CoreCompression
import swaydb.config.UncompressedBlockInfo
import swaydb.core.segment.entry.writer.EntryWriter
import swaydb.slice.{Slice, SliceMut}

private[block] class ValuesBlockState(var compressibleBytes: SliceMut[Byte],
                                      val cacheableBytes: Slice[Byte],
                                      var header: Slice[Byte],
                                      val compressions: UncompressedBlockInfo => Iterable[CoreCompression],
                                      val builder: EntryWriter.Builder) {

  def blockSize: Int =
    header.size + compressibleBytes.size

  def blockBytes =
    header ++ compressibleBytes
}
