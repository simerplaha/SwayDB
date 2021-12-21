package swaydb.core.segment.block.values

import swaydb.config.UncompressedBlockInfo
import swaydb.core.compression.CoreCompression
import swaydb.core.segment.entry.writer.EntryWriter
import swaydb.slice.{Slice, SliceMut}
import swaydb.utils.SomeOrNone

sealed trait ValuesBlockStateOption extends SomeOrNone[ValuesBlockStateOption, ValuesBlockState] {
  override def noneS: ValuesBlockStateOption =
    ValuesBlockState.Null
}

case object ValuesBlockState {

  final case object Null extends ValuesBlockStateOption {
    override def isNoneS: Boolean = true

    override def getS: ValuesBlockState = throw new Exception(s"${ValuesBlockState.productPrefix} is of type ${Null.productPrefix}")
  }
}


private[block] class ValuesBlockState(var compressibleBytes: SliceMut[Byte],
                                      val cacheableBytes: Slice[Byte],
                                      var header: Slice[Byte],
                                      val compressions: UncompressedBlockInfo => Iterable[CoreCompression],
                                      val builder: EntryWriter.Builder) extends ValuesBlockStateOption {

  override def isNoneS: Boolean =
    false

  override def getS: ValuesBlockState =
    this

  def blockSize: Int =
    header.size + compressibleBytes.size

  def blockBytes =
    header ++ compressibleBytes
}
