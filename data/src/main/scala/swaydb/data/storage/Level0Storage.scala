/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.data.storage

import swaydb.data.config.{MMAP, RecoveryMode}

import java.nio.file.Path

sealed trait Level0Storage {
  val memory: Boolean

  def persistent = !memory

  def isMMAP: Boolean
}

object Level0Storage {

  case object Memory extends Level0Storage {
    override val memory: Boolean = true
    override def isMMAP: Boolean = false
  }

  case class Persistent(mmap: MMAP.Map,
                        dir: Path,
                        recovery: RecoveryMode) extends Level0Storage {
    override val memory: Boolean = false
    override def isMMAP: Boolean = mmap.isMMAP
  }
}
