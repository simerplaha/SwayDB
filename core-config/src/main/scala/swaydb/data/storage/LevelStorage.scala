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

package swaydb.data.storage

import swaydb.data.config.MMAP
import swaydb.effect.Dir

import java.nio.file.Path

sealed trait LevelStorage {
  val dir: Path
  val memory: Boolean

  def persistent = !memory

  def dirs: Seq[Dir]
}

object LevelStorage {

  case class Memory(dir: Path) extends LevelStorage {
    override val memory: Boolean = true

    override def dirs: Seq[Dir] = Seq(Dir(dir, 1))
  }

  case class Persistent(dir: Path,
                        otherDirs: Seq[Dir],
                        appendixMMAP: MMAP.Log,
                        appendixFlushCheckpointSize: Int) extends LevelStorage {

    override val memory: Boolean = false

    override def dirs: Seq[Dir] =
      if (otherDirs.exists(_.path == dir))
        otherDirs
      else
        Dir(dir, 1) +: otherDirs
  }
}
