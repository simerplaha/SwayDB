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

package swaydb.core.level.compaction

import swaydb.core.level.zero.LevelZero
import swaydb.data.compaction.CompactionExecutionContext
import swaydb.{IO, WiredActor}

private[core] trait Compactor[S] {

  def createAndListen(zero: LevelZero,
                      executionContexts: List[CompactionExecutionContext],
                      copyForwardAllOnStart: Boolean)(implicit compaction: Compaction[S]): IO[swaydb.Error.Level, WiredActor[Compactor[S], S]]

  def wakeUp(state: S,
             forwardCopyOnAllLevels: Boolean,
             self: WiredActor[Compactor[S], S])(implicit compaction: Compaction[S]): Unit

  def terminate(self: WiredActor[Compactor[S], S]): Unit
}
