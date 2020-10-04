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

package swaydb.core.build

import java.nio.file.Path

import swaydb.Exception.{IncompatibleVersions, MissingMultiMapGenFolder}
import swaydb.core.io.file.Effect
import swaydb.data.DataType
import swaydb.{Exception, IO}

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
          IO.failed(Exception.MissingBuildInfo(Build.fileName, thisVersion.version))

        case previous @ Build.Info(previousVersion, previousDataType) =>
          val isValid = previousVersion.major >= 0 && previousVersion.minor >= 16 && previousVersion.revision >= 0
          if (!isValid)
            IO.failed(IncompatibleVersions(previous.version.version, thisVersion.version))
          else if (previousDataType != dataType)
            IO.failed(Exception.InvalidDirectoryType(dataType, previous.dataType))
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
            IO.failed(MissingMultiMapGenFolder(multiMapFolder))
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
            IO.failed(Exception.InvalidDirectoryType(dataType, DataType.MultiMap))
          else
            checkExists()
      }
  }
}
