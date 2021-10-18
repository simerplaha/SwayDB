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

package swaydb.core.build

import swaydb.Exception.{IncompatibleVersions, MissingMultiMapGenFolder}
import swaydb.data.DataType
import swaydb.effect.Effect
import swaydb.{Exception, IO}

import java.nio.file.Path

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
          val isValid = previousVersion.major >= 0 && previousVersion.minor >= 17 && previousVersion.revision >= 0
          if (!isValid)
            IO.failed(IncompatibleVersions(previous.version.version, thisVersion.version))
          else if (previousDataType != dataType)
            IO.failed(Exception.InvalidDirectoryType(dataType.name, previous.dataType.name))
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
            IO.failed(Exception.InvalidDirectoryType(dataType.name, DataType.MultiMap.name))
          else
            checkExists()
      }
  }
}
