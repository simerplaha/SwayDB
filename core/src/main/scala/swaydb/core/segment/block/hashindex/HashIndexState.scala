package swaydb.core.segment.block.hashindex

import swaydb.compression.CompressionInternal
import swaydb.data.config.UncompressedBlockInfo
import swaydb.data.slice.Slice

import scala.beans.BeanProperty

private[block] final class HashIndexState(var hit: Int,
                                          var miss: Int,
                                          val format: HashIndexEntryFormat,
                                          val minimumNumberOfKeys: Int,
                                          val minimumNumberOfHits: Int,
                                          val writeAbleLargestValueSize: Int,
                                          @BeanProperty var minimumCRC: Long,
                                          val maxProbe: Int,
                                          var compressibleBytes: Slice[Byte],
                                          val cacheableBytes: Slice[Byte],
                                          var header: Slice[Byte],
                                          val compressions: UncompressedBlockInfo => Iterable[CompressionInternal]) {

  def blockSize: Int =
    header.size + compressibleBytes.size

  def hasMinimumHits: Boolean =
    hit >= minimumNumberOfHits

  val hashMaxOffset: Int =
    compressibleBytes.allocatedSize - writeAbleLargestValueSize
}
