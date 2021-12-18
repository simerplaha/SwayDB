package swaydb.core.segment.cache.sweeper

import swaydb.ActorConfig
import swaydb.config.MemoryCache
import swaydb.configs.level.DefaultExecutionContext
import swaydb.core.segment.block.{BlockCache, BlockCacheState}
import swaydb.core.segment.cache.sweeper.MemorySweeper
import swaydb.testkit.TestKit._
import swaydb.utils.StorageUnits._

import scala.concurrent.duration._

object MemorySweeperTestKit {

  def createMemorySweeperMax(): Option[MemorySweeper.All] =
    MemorySweeper(MemoryCache.All(4096, 1.mb / 2, 600.mb, false, None, false, ActorConfig.TimeLoop("TimeLoop test", 10.seconds, DefaultExecutionContext.sweeperEC)))
      .map(_.asInstanceOf[MemorySweeper.All])

  def createMemorySweeper10(): Option[MemorySweeper.All] =
    MemorySweeper(MemoryCache.All(4096, 1.mb / 2, 600.mb, false, Some(1), false, ActorConfig.TimeLoop("TimeLoop test 2", 10.seconds, DefaultExecutionContext.sweeperEC)))
      .map(_.asInstanceOf[MemorySweeper.All])

  def createMemoryBlockSweeper(): Option[MemorySweeper.BlockSweeper] =
    MemorySweeper(MemoryCache.ByteCacheOnly(4096, 1.mb / 2, 600.mb, disableForSearchIO = false, ActorConfig.Basic("Basic Actor", DefaultExecutionContext.sweeperEC)))
      .map(_.asInstanceOf[MemorySweeper.BlockSweeper])

  def createKeyValueSweeperBlock(): Option[MemorySweeper.KeyValueSweeper] =
    MemorySweeper(MemoryCache.KeyValueCacheOnly(600.mb, Some(100), Some(ActorConfig.Basic("Basic Actor 2", DefaultExecutionContext.sweeperEC))))
      .map(_.asInstanceOf[MemorySweeper.KeyValueSweeper])

  def createMemorySweeperRandom(): Option[MemorySweeper.All] =
    eitherOne(
      createMemorySweeper10(),
      createMemorySweeperMax(),
      None
    )

  def createBlockCache(memorySweeper: Option[MemorySweeper.All]): Option[BlockCacheState] =
    BlockCache.forSearch(maxCacheSizeOrZero = 0, blockSweeper = memorySweeper)

  def createBlockCacheBlockSweeper(blockSweeper: Option[MemorySweeper.BlockSweeper] = createMemoryBlockSweeper()): Option[BlockCacheState] =
    BlockCache.forSearch(maxCacheSizeOrZero = 0, blockSweeper = blockSweeper)

  def createBlockCacheRandom(): Option[BlockCacheState] =
    eitherOne(
      createBlockCache(orNone(createMemorySweeperRandom())),
      createBlockCacheBlockSweeper(orNone(createMemoryBlockSweeper()))
    )

  def randomBlockCache: Option[BlockCacheState] =
    orNone(createBlockCache(createMemorySweeperRandom()))

  def createRandomCacheSweeper(): Option[MemorySweeper.Cache] =
    eitherOne(
      createMemorySweeperMax(),
      createMemoryBlockSweeper()
    )
}
