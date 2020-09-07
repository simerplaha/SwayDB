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

import com.typesafe.scalalogging.LazyLogging
import swaydb.IO
import swaydb.core.io.file.Effect
import swaydb.data.DataType
import swaydb.macros.VersionReader

sealed trait Build
object Build extends LazyLogging {

  val formatId = 1.toByte

  val fileName = "build.info"

  final val (major, minor, revision) = VersionReader.version

  case class Version(major: Int,
                     minor: Int,
                     revision: Int) {
    val version = s"""$major.$minor.$revision"""
  }

  final def thisVersion(): Version = Version(major, minor, revision)

  def validate[E: IO.ExceptionHandler](folder: Path)(implicit validator: BuildValidator): IO[E, (Option[DataType], Build)] =
    read(folder) flatMap {
      previousBuild =>
        validator
          .validate(previousBuild, Build.thisVersion())
          .map {
            dataType =>
              (dataType, previousBuild)
          }
    }

  def validateOrCreate[E: IO.ExceptionHandler](folder: Path)(implicit validator: BuildValidator): IO[E, Unit] =
    validate(folder) flatMap {
      case (dataType, result) =>
        result match {
          case Build.Fresh | Build.NoBuildInfo =>
            //if there is no existing Build.Info write the current build's Build.Info.
            dataType match {
              case Some(dataType) =>
                write(folder, dataType).and(IO.unit)

              case None =>
                IO.unit
            }

          case _: Build.Info =>
            //if Build.Info already exists. Do nothing.
            IO.unit
        }
    }

  def write[E: IO.ExceptionHandler](folder: Path, dataType: DataType): IO[E, Path] =
    write(folder, Build.Info(thisVersion(), dataType))

  def write[E: IO.ExceptionHandler](folder: Path, buildInfo: Build.Info): IO[E, Path] =
    IO {
      val file = folder.resolve(fileName)

      val slice = BuildSerialiser.write(buildInfo)

      Effect.createDirectoriesIfAbsent(folder)
      logger.info(s"Writing build.info - v${buildInfo.version}")
      Effect.write(file, slice)
    }

  def read[E: IO.ExceptionHandler](folder: Path): IO[E, Build] =
    Effect.isEmptyOrNotExists(folder) flatMap {
      isEmpty =>
        val file = folder.resolve(fileName)

        if (isEmpty)
          IO.Right(Build.Fresh)
        else if (Effect.notExists(file))
          IO.Right(Build.NoBuildInfo)
        else
          IO {
            val bytes = Effect.readAllBytes(file)
            val buildInfo = BuildSerialiser.read(bytes, file)
            logger.info(s"build.info - v${buildInfo.version}")
            buildInfo
          }
    }

  /**
   * Brand new database
   */
  object Fresh extends Build

  /**
   * Existing database without [[Build.Info]]
   */
  object NoBuildInfo extends Build

  /**
   * Existing database with [[Build.Info]]
   */
  case class Info private(version: Version,
                          dataType: DataType) extends Build
}
