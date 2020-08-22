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
 * If you modify this Program, or any covered work, by linking or combining
 * it with other code, such other code is not for that reason alone subject
 * to any of the requirements of the GNU Affero GPL version 3.
 */

package swaydb.core.level

import com.typesafe.scalalogging.LazyLogging
import swaydb.Bag
import swaydb.Bag._
import swaydb.core.actor.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.actor.FileSweeper.FileSweeperActor
import swaydb.core.actor.{ByteBufferSweeper, FileSweeper, MemorySweeper}
import swaydb.core.io.file.BlockCache

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

object LevelCloser extends LazyLogging {

  def closeAsync[BAG[_]](retryOnBusyDelay: FiniteDuration)(implicit keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                           blockCache: Option[BlockCache.State],
                                                           fileSweeper: FileSweeperActor,
                                                           bufferCleaner: ByteBufferSweeperActor,
                                                           bag: Bag.Async[BAG]): BAG[Unit] = {

    MemorySweeper.close(keyValueMemorySweeper)
    BlockCache.close(blockCache)

    FileSweeper.closeAsync(retryOnBusyDelay)
      .and(ByteBufferSweeper.closeAsync(retryOnBusyDelay))
  }

  def closeSync[BAG[_]](retryDelays: FiniteDuration,
                        timeout: FiniteDuration)(implicit keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                 blockCache: Option[BlockCache.State],
                                                 fileSweeper: FileSweeperActor,
                                                 bufferCleaner: ByteBufferSweeperActor,
                                                 ec: ExecutionContext,
                                                 bag: Bag.Sync[BAG]): BAG[Unit] = {

    MemorySweeper.close(keyValueMemorySweeper)
    BlockCache.close(blockCache)

    FileSweeper.closeSync[BAG](retryDelays)
      .and(ByteBufferSweeper.closeSync[BAG](retryDelays, timeout))
  }
}
