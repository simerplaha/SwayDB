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

import com.typesafe.scalalogging.LazyLogging
import swaydb.IO
import swaydb.data.DataType
import swaydb.slice.Slice
import swaydb.effect.Effect
import swaydb.macros.VersionReader

import java.nio.file.Path

sealed trait Build
object Build extends LazyLogging {

  val formatId = 1.toByte

  val fileName = "build.info"

  final val (major, minor, revision) = VersionReader.version

  case class Version(major: Int,
                     minor: Int,
                     revision: Int) {
    val version =
      if (revision == 0)
        s"""$major.$minor"""
      else
        s"""$major.$minor.$revision"""
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
      logger.debug(s"Writing build.info - v${buildInfo.version.version}")
      Effect.write(file, slice.toByteBufferWrap)
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
            val buildInfo = BuildSerialiser.read(Slice(bytes), file)
            logger.debug(s"build.info - v${buildInfo.version.version}")
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
