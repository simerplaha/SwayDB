package swaydb.core.segment.block.hashindex

import swaydb.config.UncompressedBlockInfo
import swaydb.core.compression.CoreCompression
import swaydb.core.util.CRC32
import swaydb.slice.{Slice, SliceMut}
import swaydb.utils.SomeOrNone

import scala.beans.BeanProperty

sealed trait HashIndexBlockStateOption extends SomeOrNone[HashIndexBlockStateOption, HashIndexBlockState] {
  override def noneS: HashIndexBlockStateOption =
    HashIndexBlockState.Null
}

case object HashIndexBlockState {

  final case object Null extends HashIndexBlockStateOption {
    override def isNoneS: Boolean = true

    override def getS: HashIndexBlockState = throw new Exception(s"${HashIndexBlockState.productPrefix} is of type ${Null.productPrefix}")
  }
}


private[block] final class HashIndexBlockState(var hit: Int,
                                               var miss: Int,
                                               val format: HashIndexEntryFormat,
                                               val minimumNumberOfKeys: Int,
                                               val minimumNumberOfHits: Int,
                                               val writeAbleLargestValueSize: Int,
                                               @BeanProperty var minimumCRC: Long,
                                               val maxProbe: Int,
                                               var compressibleBytes: SliceMut[Byte],
                                               val cacheableBytes: Slice[Byte],
                                               var header: Slice[Byte],
                                               val compressions: UncompressedBlockInfo => Iterable[CoreCompression]) extends HashIndexBlockStateOption {

  override def isNoneS: Boolean =
    false

  override def getS: HashIndexBlockState =
    this


  def blockSize: Int =
    header.size + compressibleBytes.size

  def hasMinimumHits: Boolean =
    hit >= minimumNumberOfHits

  //CRC can be -1 when HashIndex is not fully copied.
  def minimumCRCToWrite(): Long =
    if (minimumCRC == CRC32.disabledCRC)
      0
    else
      minimumCRC

  val hashMaxOffset: Int =
    compressibleBytes.allocatedSize - writeAbleLargestValueSize
}
