/*
 * Copyright (c) 2020 Simer Plaha (@simerplaha)
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

package swaydb.core.level.compaction

import swaydb.core.level.zero.LevelZero
import swaydb.data.compaction.CompactionExecutionContext
import swaydb.{ActorWire, IO}

import scala.concurrent.Future

private[core] trait Compactor[S] {

  def createAndListen(zero: LevelZero,
                      executionContexts: List[CompactionExecutionContext]): IO[swaydb.Error.Level, ActorWire[Compactor[S], S]]

  def wakeUp(state: S,
             forwardCopyOnAllLevels: Boolean,
             self: ActorWire[Compactor[S], S]): Unit

  def terminate(state: S, self: ActorWire[Compactor[S], S]): Future[Unit]
}
