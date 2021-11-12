package swaydb.core.segment.block

import swaydb.data.slice.{Slice, SliceOption}

class BlockCompressionResult(val compressedBytes: SliceOption[Byte],
                             val headerBytes: Slice[Byte]) {
  /**
   * More bytes could get allocated to headerBytes when it's created. This fixes the size to it's original size.
   * currently not more that 1 byte is required to store the size of the header.
   *
   * @return mutates and sets the size of [[headerBytes]]
   */
  def fixHeaderSize(): Unit = {
    headerBytes moveWritePosition 0
    headerBytes addUnsignedInt (headerBytes.size - 1)
  }
}
