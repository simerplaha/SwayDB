/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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

import swaydb.core.CommonAssertions._
import swaydb.core.actor.{FileSweeper, MemorySweeper}
import swaydb.core.io.file.BlockCache
import swaydb.data.config.{ActorConfig, MemoryCache}
import swaydb.data.util.StorageUnits._

import scala.concurrent.duration._

private[swaydb] object TestSweeper {

  implicit val level0PushDownPool = TestExecutionContext.executionContext

  val memorySweeperMax: Option[MemorySweeper.All] =
    MemorySweeper(MemoryCache.All(4098, 1.mb / 2, 600.mb, None, false, ActorConfig.TimeLoop("TimeLoop test", 10.seconds, level0PushDownPool)))
      .map(_.asInstanceOf[MemorySweeper.All])

  val memorySweeper10: Option[MemorySweeper.All] =
    MemorySweeper(MemoryCache.All(4098, 1.mb / 2, 600.mb, Some(1), false, ActorConfig.TimeLoop("TimeLoop test 2", 10.seconds, level0PushDownPool)))
      .map(_.asInstanceOf[MemorySweeper.All])

  val memorySweeperBlock: Option[MemorySweeper.BlockSweeper] =
    MemorySweeper(MemoryCache.ByteCacheOnly(4098, 1.mb / 2, 600.mb, ActorConfig.Basic("Basic Actor",level0PushDownPool)))
      .map(_.asInstanceOf[MemorySweeper.BlockSweeper])

  val keyValueSweeperBlock: Option[MemorySweeper.KeyValueSweeper] =
    MemorySweeper(MemoryCache.KeyValueCacheOnly(600.mb, Some(100), Some(ActorConfig.Basic("Basic Actor 2", level0PushDownPool))))
      .map(_.asInstanceOf[MemorySweeper.KeyValueSweeper])

  val someMemorySweeperMax = memorySweeperMax
  val someMemorySweeper10 = memorySweeper10

  val blockCache: Option[BlockCache.State] =
    memorySweeperMax.map(BlockCache.init)

  def randomBlockCache: Option[BlockCache.State] =
    orNone(blockCache)

  val fileSweeper: FileSweeper.Enabled =
    FileSweeper(50, ActorConfig.Basic("Basic test 3", level0PushDownPool))
}
