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

package swaydb.core.compaction

import swaydb.core.level.zero.LevelZero
import swaydb.core.file.sweeper.FileSweeper
import swaydb.config.compaction.CompactionConfig
import swaydb.{DefActor, IO}

/**
 * Creates Compaction Actors.
 */
private[core] trait CompactorCreator {

  def createAndListen(zero: LevelZero,
                      compactionConfig: CompactionConfig)(implicit fileSweeper: FileSweeper.On): IO[swaydb.Error.Level, DefActor[Compactor]]

}
