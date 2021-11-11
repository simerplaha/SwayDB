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

package swaydb.macros

import java.nio.file.{Files, Paths}
import scala.jdk.CollectionConverters._
import scala.language.experimental.macros
import scala.reflect.macros.blackbox

object VersionReader {

  def version: (Int, Int, Int) = macro versionImpl

  def versionImpl(c: blackbox.Context): c.Expr[(Int, Int, Int)] = {
    import c.universe._

    val _macroDirectory = this.getClass.getResource(".").getPath

    val macroDirectory =
      if (System.getProperty("os.name").toLowerCase.startsWith("win") && _macroDirectory.startsWith("/")) //windows starts path with a slash /C:/..
        _macroDirectory.drop(1)
      else
        _macroDirectory

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
    val regex = """(\d+)\.(\d+)\.?(\d+)?""".r

    val (major, minor, revision) =
      regex.findFirstMatchIn(versionString) match {
        case Some(value) =>
          val major = value.group(1)
          val minor = value.group(2)
          val revision = value.group(3)
          if (revision == null)
            (major.toInt, minor.toInt, 0)
          else
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
