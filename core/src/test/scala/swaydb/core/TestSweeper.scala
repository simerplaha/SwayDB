/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb.core

import swaydb.ActorConfig
import swaydb.configs.level.DefaultExecutionContext
import swaydb.core.CommonAssertions._
import swaydb.core.segment.block.{BlockCache, BlockCacheState}
import swaydb.core.file.sweeper.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.file.sweeper.{ByteBufferSweeper, FileSweeper}
import swaydb.core.segment.cache.sweeper.MemorySweeper
import swaydb.config.MemoryCache
import swaydb.utils.StorageUnits._

import scala.concurrent.duration._

private[swaydb] object TestSweeper {

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

  def createFileSweeper(): FileSweeper.On =
    FileSweeper(1000, ActorConfig.Basic("Basic test 3", DefaultExecutionContext.sweeperEC))

  def createBufferCleaner(): ByteBufferSweeperActor =
    ByteBufferSweeper()(DefaultExecutionContext.sweeperEC)

  def createRandomCacheSweeper(): Option[MemorySweeper.Cache] =
    eitherOne(
      createMemorySweeperMax(),
      createMemoryBlockSweeper()
    )
}
