package swaydb.core.segment.block

import swaydb.core.cache.CacheNoIO
import swaydb.core.segment.cache.sweeper.MemorySweeper
import swaydb.utils.HashedMap
import swaydb.slice.{Slice, SliceOption}

class BlockCacheState(val blockSize: Int,
                      val skipBlockCacheSeekSize: Int,
                      val sweeper: MemorySweeper.Block,
                      val mapCache: CacheNoIO[Unit, HashedMap.Concurrent[Long, SliceOption[Byte], Slice[Byte]]]) {
  val blockSizeDouble: Double = blockSize

  def clear() =
    mapCache.clear()
}
