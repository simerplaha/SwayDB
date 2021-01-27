/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * This file is a part of SwayDB.
 *
 * SwayDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * SwayDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.core

import swaydb.configs.level.DefaultExecutionContext
import swaydb.core.CommonAssertions._
import swaydb.core.sweeper.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.sweeper.MemorySweeper
import swaydb.core.segment.block.BlockCache
import swaydb.core.sweeper.{ByteBufferSweeper, FileSweeper, MemorySweeper}
import swaydb.data.config.{ActorConfig, MemoryCache}
import swaydb.data.util.StorageUnits._

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

  def createBlockCache(memorySweeper: Option[MemorySweeper.All]): Option[BlockCache.State] =
    BlockCache.forSearch(maxCacheSizeOrZero = 0, blockSweeper = memorySweeper)

  def createBlockCacheBlockSweeper(blockSweeper: Option[MemorySweeper.BlockSweeper] = createMemoryBlockSweeper()): Option[BlockCache.State] =
    BlockCache.forSearch(maxCacheSizeOrZero = 0, blockSweeper = blockSweeper)

  def createBlockCacheRandom(): Option[BlockCache.State] =
    eitherOne(
      createBlockCache(orNone(createMemorySweeperRandom())),
      createBlockCacheBlockSweeper(orNone(createMemoryBlockSweeper()))
    )

  def randomBlockCache: Option[BlockCache.State] =
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
