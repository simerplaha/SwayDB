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

package swaydb.core

import scala.beans.BeanProperty

object CoreState {
  sealed trait State {
    val isRunning: Boolean
    def isNotRunning: Boolean =
      !isRunning
  }

  case object Running extends State {
    override val isRunning: Boolean = true
  }

  case object Closing extends State {
    override val isRunning: Boolean = false
  }

  case object Closed extends State {
    override val isRunning: Boolean = false
  }
}

case class CoreState(@BeanProperty @volatile var state: CoreState.State = CoreState.Running) {
  @inline def isRunning = state.isRunning

  @inline def isNotRunning = state.isNotRunning
}
