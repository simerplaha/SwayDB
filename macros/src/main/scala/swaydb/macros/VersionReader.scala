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

package swaydb.macros

import java.nio.file.{Files, Paths}

import scala.jdk.CollectionConverters._
import scala.language.experimental.macros
import scala.reflect.macros.blackbox

object VersionReader {

  def version: (Int, Int, Int) = macro versionImpl

  def versionImpl(c: blackbox.Context): c.Expr[(Int, Int, Int)] = {
    import c.universe._

    val macroDirectory = this.getClass.getResource(".").getPath
    if (macroDirectory == null)
      c.abort(
        c.enclosingPosition,
        s"Failed to get version.sbt directory."
      )

    val rootDirectory = macroDirectory.replaceAll("macros.+", "")
    if (rootDirectory == null || rootDirectory.isEmpty)
      c.abort(
        c.enclosingPosition,
        s"Failed to project root directory. $rootDirectory."
      )

    val filePath = Paths.get(rootDirectory).resolve("version.sbt")
    if (Files.notExists(filePath))
      c.abort(
        c.enclosingPosition,
        s"version.sbt not found in $filePath."
      )

    val lines = Files.readAllLines(filePath).asScala

    assert(lines.size == 1, s"${lines.size} != 1")

    val versionString =
      """"(.+)"""".r.findFirstMatchIn(lines.head) match {
        case Some(version) =>
          val versionString = version.group(0)
          versionString

        case None =>
          c.abort(
            c.enclosingPosition,
            s"Failed to read version number from file $filePath"
          )
      }

    //get major.minor.revision from version.
    val regex = """(\d+)\.(\d+)\.(\d+)""".r

    val (major, minor, revision) =
      regex.findFirstMatchIn(versionString) match {
        case Some(value) =>
          val major = value.group(1)
          val minor = value.group(2)
          val revision = value.group(3)
          (major.toInt, minor.toInt, revision.toInt)

        case None =>
          c.abort(
            c.enclosingPosition,
            s"Invalid version string: $versionString"
          )
      }

    val tuple = q"${(major, minor, revision)}"

    c.Expr[(Int, Int, Int)](tuple)
  }
}
