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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.data

sealed trait Atomic {
  def enabled: Boolean
}

/**
 * [[Atomic.Enabled]] ensures that all range operations and [[swaydb.Prepare]] transactions
 * are available for reads atomically.
 *
 * For eg: if you submit a [[swaydb.Prepare]]
 * that updates 10 key-values, those 10 key-values will be visible to reads only after
 * each update is applied.
 */

object Atomic {

  val enabled: Atomic.Enabled = Atomic.Enabled
  val disabled: Atomic.Disabled = Atomic.Disabled

  sealed trait Enabled extends Atomic
  case object Enabled extends Enabled {
    override val enabled: Boolean = true
  }

  sealed trait Disabled extends Atomic
  case object Disabled extends Disabled {
    override val enabled: Boolean = false
  }
}
