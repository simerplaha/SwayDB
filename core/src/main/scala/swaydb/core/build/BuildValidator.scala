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

package swaydb.core.build

import swaydb.IO
import swaydb.data.DataType

sealed trait BuildValidator { self =>

  def dataType: DataType

  def validate[E: IO.ExceptionHandler](previousBuild: Build,
                                       thisVersion: Build.Version): IO[E, Unit]
}

object BuildValidator {

  case class Ignore(dataType: DataType) extends BuildValidator {
    override def validate[E: IO.ExceptionHandler](previousBuild: Build, thisVersion: Build.Version): IO[E, Unit] =
      IO.unit
  }

  /**
   * This validate does not allow SwayDB boot-up on version that are older than 0.14.0 or
   * if no build.info exist.
   */
  case class DisallowOlderVersions(dataType: DataType) extends BuildValidator {
    override def validate[E: IO.ExceptionHandler](previousBuild: Build, thisVersion: Build.Version): IO[E, Unit] =
      previousBuild match {
        case Build.Fresh =>
          IO.unit

        case Build.NoBuildInfo =>
          IO.failed(s"This directory is not empty or is an older version of SwayDB which is incompatible with v${thisVersion.version}.")

        case previous @ Build.Info(previousVersion, previousDataType) =>
          val isValid = previousVersion.major >= 0 && previousVersion.minor >= 14 && previousVersion.revision >= 0
          if (!isValid)
            IO.failed(s"Incompatible versions! v${previous.version} not compatible with v${thisVersion.version}.")
          else if (previousDataType != dataType)
            IO.failed(s"Invalid data-type! This directory is of type ${previous.dataType.name} and not ${dataType.name}.")
          else
            IO.unit
      }
  }
}
