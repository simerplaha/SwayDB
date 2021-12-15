package swaydb.core.segment.block

import swaydb.core.compression.CoreDecompressor
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

  @inline def apply(decompressor: CoreDecompressor,
                    decompressedLength: Int): BlockCompressionInfo =
    new BlockCompressionInfo(decompressor, decompressedLength)

}

class BlockCompressionInfo(val decompressor: CoreDecompressor,
                           val decompressedLength: Int) extends BlockCompressionInfoOption {
  override def isNoneS: Boolean = false
  override def getS: BlockCompressionInfo = this
}
