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

package swaydb.utils

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
