package swaydb.core.segment.block

import swaydb.core.segment.block.BlockCache.seekSize
import swaydb.slice.{Slice, SliceRO, Slices}

sealed trait BlockCacheIO {

  def seek(keyPosition: Int,
           size: Int,
           source: BlockCacheSource,
           state: BlockCacheState): SliceRO[Byte]

}

object BlockCacheIO {

  implicit case object DefaultBlockCacheIO extends BlockCacheIO {
    def seek(keyPosition: Int,
             size: Int,
             source: BlockCacheSource,
             state: BlockCacheState): SliceRO[Byte] = {
      val seekedSize =
        seekSize(
          lowerFilePosition = keyPosition,
          size = size,
          source = source,
          state = state
        )

      val bytes =
        source
          .readFromSource(
            position = keyPosition,
            size = seekedSize,
            blockSize = state.blockSize
          )

      //      diskSeeks += 1
      if (state.blockSize <= 0)
        bytes
      else if (bytes.isEmpty)
        Slice.emptyBytes
      else
        bytes match {
          case bytes: Slice[Byte] =>
            val map = state.mapCache.value(())
            map.put(keyPosition, bytes)
            state.sweeper.add(keyPosition, bytes, state.mapCache)
            bytes

          case bytes: Slices[Byte] =>
            val map = state.mapCache.value(())
            var index = 0
            var position = keyPosition
            while (index < bytes.slices.length) {
              val slice = bytes.slices(index)
              map.put(position, slice)
              state.sweeper.add(position, slice, state.mapCache)
              position = position + slice.size
              index += 1
            }
            bytes
        }
    }
  }
}
