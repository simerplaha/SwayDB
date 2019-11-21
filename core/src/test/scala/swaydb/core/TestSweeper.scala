/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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
 */

package swaydb.core

import java.util.concurrent.ConcurrentLinkedQueue

import swaydb.core.CommonAssertions._
import swaydb.core.TestData._
import swaydb.core.actor.{FileSweeper, FileSweeperItem, MemorySweeper}
import swaydb.core.io.file.BlockCache
import swaydb.data.config.{ActorConfig, MemoryCache}
import swaydb.data.util.StorageUnits._

import scala.concurrent.duration._

private[swaydb] object TestSweeper {

  implicit val level0PushDownPool = TestExecutionContext.executionContext

  val memorySweeperMax: Option[MemorySweeper.All] =
    MemorySweeper(MemoryCache.All(4098, 1.mb / 2, 600.mb, None, false, ActorConfig.TimeLoop(10.seconds, level0PushDownPool)))
      .map(_.asInstanceOf[MemorySweeper.All])

  val memorySweeper10: Option[MemorySweeper.All] =
    MemorySweeper(MemoryCache.All(4098, 1.mb / 2, 600.mb, Some(1), false, ActorConfig.TimeLoop(10.seconds, level0PushDownPool)))
      .map(_.asInstanceOf[MemorySweeper.All])

  val memorySweeperBlock: Option[MemorySweeper.BlockSweeper] =
    MemorySweeper(MemoryCache.ByteCacheOnly(4098, 1.mb / 2, 600.mb, ActorConfig.Basic(level0PushDownPool)))
      .map(_.asInstanceOf[MemorySweeper.BlockSweeper])

  val keyValueSweeperBlock: Option[MemorySweeper.KeyValueSweeper] =
    MemorySweeper(MemoryCache.KeyValueCacheOnly(600.mb, randomIntMaxOption(10000), Some(ActorConfig.Basic(level0PushDownPool))))
      .map(_.asInstanceOf[MemorySweeper.KeyValueSweeper])

  val someMemorySweeperMax = memorySweeperMax
  val someMemorySweeper10 = memorySweeper10

  val closeQueue = new ConcurrentLinkedQueue[FileSweeperItem]()
  @volatile var closeQueueSize = closeQueue.size()

  val deleteQueue = new ConcurrentLinkedQueue[FileSweeperItem]()
  @volatile var deleteQueueSize = closeQueue.size()

  val blockCache: Option[BlockCache.State] =
    memorySweeperMax.map(BlockCache.init)

  def randomBlockCache: Option[BlockCache.State] =
    orNone(blockCache)

  val fileSweeper: FileSweeper.Enabled =
    FileSweeper(100, ActorConfig.TimeLoop(10.seconds, level0PushDownPool))
}
