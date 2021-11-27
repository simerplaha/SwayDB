package swaydb.core.segment.block

import swaydb.core.compression.DecompressorInternal
import swaydb.utils.SomeOrNone

/**
 * Optional [[BlockCompressionInfo]] where the None value is [[BlockCompressionInfo.Null]]
 */
sealed trait BlockCompressionInfoOption extends SomeOrNone[BlockCompressionInfoOption, BlockCompressionInfo] {
  override def noneS: BlockCompressionInfoOption = BlockCompressionInfo.Null
}

case object BlockCompressionInfo {

  case object Null extends BlockCompressionInfoOption {
    override def isNoneS: Boolean =
      true

    override def getS: BlockCompressionInfo =
      throw new Exception(s"${BlockCompressionInfo.productPrefix} is ${Null.productPrefix}")
  }

  @inline def apply(decompressor: DecompressorInternal,
                    decompressedLength: Int): BlockCompressionInfo =
    new BlockCompressionInfo(decompressor, decompressedLength)

}

class BlockCompressionInfo(val decompressor: DecompressorInternal,
                           val decompressedLength: Int) extends BlockCompressionInfoOption {
  override def isNoneS: Boolean = false
  override def getS: BlockCompressionInfo = this
}
