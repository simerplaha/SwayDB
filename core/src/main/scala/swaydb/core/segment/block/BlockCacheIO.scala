package swaydb.core.segment.block

import swaydb.core.segment.block.BlockCache.seekSize
import swaydb.data.slice.Slice

sealed trait BlockCacheIO {

  def seek(keyPosition: Int,
           size: Int,
           source: BlockCacheSource,
           state: BlockCacheState): Slice[Byte]

}

object BlockCacheIO {

  implicit case object DefaultBlockCacheIO extends BlockCacheIO {
    def seek(keyPosition: Int,
             size: Int,
             source: BlockCacheSource,
             state: BlockCacheState): Slice[Byte] = {
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
            size = seekedSize
          )

      //      diskSeeks += 1
      if (state.blockSize <= 0) {
        bytes
      } else if (bytes.isEmpty) {
        Slice.emptyBytes
      } else if (bytes.size <= state.blockSize) {
        val value = bytes.unslice()
        val map = state.mapCache.value(())
        map.put(keyPosition, value)
        state.sweeper.add(keyPosition, value, state.mapCache)
        bytes
      } else {
        //        splitsCount += 1
        var index = 0
        var position = keyPosition
        val splits = Math.ceil(bytes.size / state.blockSizeDouble)
        val map = state.mapCache.value(())
        while (index < splits) {
          val bytesToPut = bytes.take(index * state.blockSize, state.blockSize)
          map.put(position, bytesToPut)
          state.sweeper.add(position, bytesToPut, state.mapCache)
          position = position + bytesToPut.size
          index += 1
        }
        bytes
      }
    }
  }

}
