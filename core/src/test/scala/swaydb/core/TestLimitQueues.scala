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

import swaydb.core.queue.{FileSweeper, FileSweeperItem, MemorySweeper}
import swaydb.data.util.StorageUnits._

import scala.concurrent.duration._
import CommonAssertions._
import swaydb.core.io.file.BlockCache

object TestLimitQueues {

  implicit val level0PushDownPool = TestExecutionContext.executionContext

  val memorySweeper = MemorySweeper(10.mb, 5.seconds)

  val closeQueue = new ConcurrentLinkedQueue[FileSweeperItem]()
  @volatile var closeQueueSize = closeQueue.size()

  val deleteQueue = new ConcurrentLinkedQueue[FileSweeperItem]()
  @volatile var deleteQueueSize = closeQueue.size()

  val blockCache: Option[BlockCache.State] =
    Some(BlockCache.init(100.mb))

  def randomBlockCache: Option[BlockCache.State] =
    orNone(blockCache)

  val fileSweeper: FileSweeper =
    new FileSweeper {
      override def close(file: FileSweeperItem): Unit = {
        closeQueue.add(file)
        closeQueueSize += 1
        if (closeQueueSize > 1000) {
          closeQueue.poll().close().get
        }
      }

      override def delete(file: FileSweeperItem): Unit = {
        deleteQueue.add(file)
        deleteQueueSize += 1
        if (deleteQueueSize > 100) {
          closeQueue.poll().delete().get
        }
      }

      override def terminate(): Unit = ()
    }
}
