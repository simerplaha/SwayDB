/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.segment.format.one.entry.generators

import java.io.PrintWriter
import java.nio.file.Paths

import swaydb.core.io.file.IO

import scala.io.Source

/**
  * Generates if Ids for all key-value types other than [[swaydb.core.segment.format.one.entry.id.RemoveEntryId]]
  * which does not contain a value.
  */
object IdsGenerator extends App {

  def write(entry: String,
            key: String,
            startId: Int): Int = {
    val fullKeyId = s"${entry}Key${key}EntryId"
    val targetIdClass = IO.createFileIfAbsent(Paths.get(s"${System.getProperty("user.dir")}/core/src/main/scala/swaydb/core/segment/format/one/entry/id/$fullKeyId.scala")).get
    val writer = new PrintWriter(targetIdClass.toFile)
    writer.write("")

    val maxId =
      Source
        .fromFile(s"${System.getProperty("user.dir")}/core/src/main/scala/swaydb/core/segment/format/one/entry/generators/TemplateEntryId.scala")
        .getLines
        .foldLeft(startId) {
          case (id, line) =>
            val newLine =
              if (!line.contains("remove this")) {
                val newLine =
                  line
                    .replace("package swaydb.core.segment.format.one.entry.generators", "package swaydb.core.segment.format.one.entry.id")
                    .replace("Key.Uncompressed", s"Key.$key")
                    .replace("KeyUncompressed", s"Key$key")
                    .replace("TemplateEntryId", fullKeyId)
                    .replace(s"-1", s"$id")
                writer.append("\n" + newLine)
                newLine
              } else {
                ""
              }

            if (line.contains("package")) {
              writer.append("\n")
              writer.append(
                """
                  |/** ******************************************
                  |  * ************ GENERATED CLASS *************
                  |  * ******************************************/""".stripMargin)
            }

            if (newLine.contains(s"$fullKeyId($id)"))
              id + 1
            else
              id
        }
    writer.close()
    maxId
  }

  def entries = Seq("Put", "Group", "Range", "Update")
  def keys = Seq("PartiallyCompressed", "Uncompressed", "FullyCompressed")

  def maxKey =
    entries.foldLeft(30) {
      case (id, entry) =>
        keys.foldLeft(id) {
          case (id, key) =>
            write(entry, key, id) + 1
        }
    }

  println("maxKey: " + maxKey)
}

