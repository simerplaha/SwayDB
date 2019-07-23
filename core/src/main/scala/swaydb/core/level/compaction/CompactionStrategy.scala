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

import swaydb.IO
import swaydb.core.actor.WiredActor
import swaydb.core.level.zero.LevelZero
import swaydb.data.compaction.CompactionExecutionContext

private[core] trait CompactionStrategy[S] {

  def createAndListen(zero: LevelZero,
                      executionContexts: List[CompactionExecutionContext],
                      copyForwardAllOnStart: Boolean)(implicit compactionOrdering: CompactionOrdering): IO[IO.Error, WiredActor[CompactionStrategy[CompactorState], CompactorState]]

  def wakeUp(state: S,
             forwardCopyOnAllLevels: Boolean,
             self: WiredActor[CompactionStrategy[S], S]): Unit

  def terminate(self: WiredActor[CompactionStrategy[CompactorState], CompactorState]): Unit
}
