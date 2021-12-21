package swaydb.core.segment.block.bloomfilter

import swaydb.config.UncompressedBlockInfo
import swaydb.core.compression.CoreCompression
import swaydb.slice.{Slice, SliceMut}
import swaydb.utils.SomeOrNone

sealed trait BloomFilterBlockStateOption extends SomeOrNone[BloomFilterBlockStateOption, BloomFilterBlockState] {
  override def noneS: BloomFilterBlockStateOption =
    BloomFilterBlockState.Null
}

case object BloomFilterBlockState {

  final case object Null extends BloomFilterBlockStateOption {
    override def isNoneS: Boolean = true

    override def getS: BloomFilterBlockState = throw new Exception(s"${BloomFilterBlockState.productPrefix} is of type ${Null.productPrefix}")
  }
}


private[core] class BloomFilterBlockState(val numberOfBits: Int,
                                          val maxProbe: Int,
                                          var compressibleBytes: SliceMut[Byte],
                                          val cacheableBytes: Slice[Byte],
                                          var header: Slice[Byte],
                                          val compressions: UncompressedBlockInfo => Iterable[CoreCompression]) extends BloomFilterBlockStateOption {

  override def isNoneS: Boolean =
    false

  override def getS: BloomFilterBlockState =
    this

  def blockSize: Int =
    header.size + compressibleBytes.size

  def blockBytes: Slice[Byte] =
    header ++ compressibleBytes

  def written: Int =
    compressibleBytes.size

  override def hashCode(): Int =
    compressibleBytes.hashCode()
}
