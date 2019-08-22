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
import swaydb.core.io.file.BlockCache
import swaydb.core.queue.{FileSweeper, FileSweeperItem, MemorySweeper}
import swaydb.data.config.{ActorQueue, MemoryCache}
import swaydb.data.util.StorageUnits._

object TestLimitQueues {

  implicit val level0PushDownPool = TestExecutionContext.executionContext

  val memorySweeper: Option[MemorySweeper.Both] =
    MemorySweeper(MemoryCache.EnableBoth(4098, 10.mb, ActorQueue.Basic(10000, level0PushDownPool)))
      .map(_.asInstanceOf[MemorySweeper.Both])

  val someMemorySweeper = memorySweeper

  val closeQueue = new ConcurrentLinkedQueue[FileSweeperItem]()
  @volatile var closeQueueSize = closeQueue.size()

  val deleteQueue = new ConcurrentLinkedQueue[FileSweeperItem]()
  @volatile var deleteQueueSize = closeQueue.size()

  val blockCache: Option[BlockCache.State] =
    memorySweeper.map(BlockCache.init)

  def randomBlockCache: Option[BlockCache.State] =
    orNone(blockCache)

  val fileSweeper: FileSweeper.Enabled =
    FileSweeper(1000, ActorQueue.Basic(1000, level0PushDownPool))
}
