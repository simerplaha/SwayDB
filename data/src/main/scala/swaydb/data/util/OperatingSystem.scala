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

sealed trait OperatingSystem

object OperatingSystem {

  @volatile private var operatingSystem: Option[OperatingSystem] = None

  case object Windows extends OperatingSystem
  case object Mac extends OperatingSystem
  case object Other extends OperatingSystem

  def isWindows: Boolean =
    get() == OperatingSystem.Windows

  def isNotWindows: Boolean =
    !isWindows

  def isMac: Boolean =
    get() == OperatingSystem.Mac

  def isNotMac: Boolean =
    !isMac

  def isOther: Boolean =
    get() == OperatingSystem.Other

  def isNotOther: Boolean =
    !isOther

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
