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

import java.nio.file.Path

import swaydb.IO
import swaydb.core.io.file.Effect
import swaydb.data.DataType

sealed trait BuildValidator { self =>

  def validate[E: IO.ExceptionHandler](previousBuild: Build,
                                       thisVersion: Build.Version): IO[E, Option[DataType]]

  def and(other: BuildValidator): BuildValidator =
    new BuildValidator {
      override def validate[E: IO.ExceptionHandler](previousBuild: Build, thisVersion: Build.Version): IO[E, Option[DataType]] =
        self.validate(
          previousBuild = previousBuild,
          thisVersion = thisVersion
        ) flatMap {
          previousDataType =>
            other.validate(
              previousBuild = previousBuild,
              thisVersion = thisVersion
            ) transform {
              nextDataType =>
                nextDataType orElse previousDataType
            }
        }
    }
}

object BuildValidator {

  def ignore(): BuildValidator.Ignore =
    BuildValidator.Ignore

  sealed trait Ignore extends BuildValidator
  case object Ignore extends Ignore {
    override def validate[E: IO.ExceptionHandler](previousBuild: Build, thisVersion: Build.Version): IO[E, Option[DataType]] =
      IO.none
  }

  /**
   * This validate does not allow SwayDB boot-up on version that are older than 0.14.0 or
   * if no build.info exist.
   */
  case class DisallowOlderVersions(dataType: DataType) extends BuildValidator {
    override def validate[E: IO.ExceptionHandler](previousBuild: Build, thisVersion: Build.Version): IO[E, Option[DataType]] =
      previousBuild match {
        case Build.Fresh =>
          IO(Some(dataType))

        case Build.NoBuildInfo =>
          IO.failed(s"Missing ${Build.fileName} file. This directory might be an incompatible older version of SwayDB. Current version: v${thisVersion.version}.")

        case previous @ Build.Info(previousVersion, previousDataType) =>
          val isValid = previousVersion.major >= 0 && previousVersion.minor >= 14 && previousVersion.revision >= 0
          if (!isValid)
            IO.failed(s"Incompatible versions! v${previous.version} is not compatible with v${thisVersion.version}.")
          else if (previousDataType != dataType)
            IO.failed(s"Invalid type ${dataType.name}. This directory is of type ${previous.dataType.name}.")
          else
            IO(Some(dataType))
      }
  }

  /**
   * Validates that a MultiMap has the multimap folder with log file.
   */
  case class MultiMapFileExists(multiMapFolder: Path) extends BuildValidator {

    def checkExists[E: IO.ExceptionHandler](): IO[E, Option[DataType]] =
      Effect.isEmptyOrNotExists(multiMapFolder) match {
        case IO.Right(isEmpty) =>
          if (isEmpty)
            IO.failed(s"Missing multimap gen file or folder: $multiMapFolder.")
          else
            IO.none

        case IO.Left(value) =>
          IO.Left(value)
      }

    override def validate[E: IO.ExceptionHandler](previousBuild: Build, thisVersion: Build.Version): IO[E, Option[DataType]] =
      previousBuild match {
        case Build.Fresh =>
          IO.none

        case Build.NoBuildInfo =>
          checkExists()

        case Build.Info(_, dataType) =>
          if (dataType != DataType.MultiMap)
            IO.failed(s"Invalid data-type ${dataType.name}. Expected ${DataType.MultiMap.name}.")
          else
            checkExists()
      }
  }
}