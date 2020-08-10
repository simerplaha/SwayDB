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

package swaydb.data.util

sealed trait OperatingSystem {
  def isWindows: Boolean

  def isMac: Boolean

  def isOther: Boolean
}

object OperatingSystem {

  @volatile private var operatingSystem: Option[OperatingSystem] = None

  case object Windows extends OperatingSystem {
    override val isWindows: Boolean = true
    override val isMac: Boolean = false
    override val isOther: Boolean = false
  }

  case object Mac extends OperatingSystem {
    override val isWindows: Boolean = false
    override val isMac: Boolean = true
    override val isOther: Boolean = false
  }

  case object Other extends OperatingSystem {
    override val isWindows: Boolean = false
    override val isMac: Boolean = false
    override val isOther: Boolean = true
  }

  def isWindows: Boolean =
    get().isWindows

  def get(): OperatingSystem =
    operatingSystem getOrElse getFromProperty()

  def getFromProperty(): OperatingSystem = {
    val osName = System.getProperty("os.name").toLowerCase

    val os =
      if (osName.startsWith("win"))
        Windows
      else if (osName.startsWith("mac"))
        Mac
      else
        Other

    this.operatingSystem = Some(os)

    os
  }
}
