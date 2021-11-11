package swaydb.core.segment.block

import swaydb.core.sweeper.MemorySweeper
import swaydb.core.util.HashedMap
import swaydb.data.cache.CacheNoIO
import swaydb.data.slice.{Slice, SliceOption}

class BlockCacheState(val blockSize: Int,
                      val skipBlockCacheSeekSize: Int,
                      val sweeper: MemorySweeper.Block,
                      val mapCache: CacheNoIO[Unit, HashedMap.Concurrent[Long, SliceOption[Byte], Slice[Byte]]]) {
  val blockSizeDouble: Double = blockSize

  def clear() =
    mapCache.clear()
}
