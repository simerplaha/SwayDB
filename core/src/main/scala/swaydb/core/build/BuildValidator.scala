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

sealed trait BuildValidator { self =>

  def validate[E: IO.ExceptionHandler](previousBuild: Build,
                                       thisBuild: Build.Info): IO[E, Unit]

  def and(validator: BuildValidator): BuildValidator =
    new BuildValidator {
      override def validate[E: IO.ExceptionHandler](previousBuild: Build, thisBuild: Build.Info): IO[E, Unit] =
        self.validate(previousBuild, thisBuild) and validator.validate(previousBuild, thisBuild)
    }
}

object BuildValidator {

  @deprecated("This validator will be removed from next version 0.14.9")
  def ignoreUnsafe(): BuildValidator.IgnoreUnsafe =
    IgnoreUnsafe

  @deprecated("This validator will be removed from next version 0.14.9")
  sealed trait IgnoreUnsafe extends BuildValidator

  /**
   * [[IgnoreUnsafe]] is available temporarily to allow running previously created database.
   * v0.14.8 does not support old [[MultiMap]] instances.
   */
  @deprecated("This validator will be removed from next version 0.14.9")
  object IgnoreUnsafe extends IgnoreUnsafe {
    override def validate[E: IO.ExceptionHandler](previousBuild: Build, thisBuild: Build.Info): IO[E, Unit] =
      IO.unit
  }

  /**
   * This validate does not allow SwayDB boot-up on version that are older than 0.14.0 or
   * if no build.info exist.
   */
  object DisallowOlderVersions extends BuildValidator {
    override def validate[E: IO.ExceptionHandler](previousBuild: Build, thisBuild: Build.Info): IO[E, Unit] =
      previousBuild match {
        case Build.Fresh =>
          IO.unit

        case Build.NoBuildInfo =>
          IO.failed(s"Previous SwayDB version (version number unknown) is not compatible with v${thisBuild.version}. v0.14.8 " +
            s"added incompatibility check which requires build.info file that did not exist in older versions. " +
            s"If you would like to ignore the incompatibility check you can supply BuildValidator.IgnoreUnsafe to your " +
            s"SwayDB instance. Note that BuildValidator.IgnoreUnsafe will be removed from v0.14.9.")

        case previous @ Build.Info(major, minor, revision) =>
          val isValid = major >= 0 && minor >= 14 && revision >= 0
          if (!isValid)
            IO.failed(s"SwayDB v${previous.version} not compatible with v${thisBuild.version}.")
          else
            IO.unit
      }
  }
}
