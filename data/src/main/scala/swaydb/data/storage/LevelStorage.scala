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

package swaydb.data.storage

import java.nio.file.Path

import swaydb.data.config.Dir

sealed trait LevelStorage {
  val dir: Path
  val memory: Boolean

  def persistent = !memory

  val mmapSegmentsOnWrite: Boolean
  val mmapSegmentsOnRead: Boolean

  def dirs: Seq[Dir]
}

object LevelStorage {

  case class Memory(dir: Path) extends LevelStorage {
    override val memory: Boolean = true
    override val mmapSegmentsOnWrite: Boolean = false
    override val mmapSegmentsOnRead: Boolean = false

    override def dirs: Seq[Dir] = Seq(Dir(dir, 1))
  }

  case class Persistent(mmapSegmentsOnWrite: Boolean,
                        mmapSegmentsOnRead: Boolean,
                        dir: Path,
                        otherDirs: Seq[Dir]) extends LevelStorage {
    override val memory: Boolean = false

    override def dirs: Seq[Dir] =
      if (otherDirs.exists(_.path == dir))
        otherDirs
      else
        Dir(dir, 1) +: otherDirs
  }
}