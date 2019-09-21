/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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
 */

package swaydb.core.segment.format.a.entry.generators

import java.nio.file.Paths

import swaydb.core.io.file.Effect
import swaydb.core.segment.format.a.entry.id._
import swaydb.data.slice.Slice

import scala.collection.mutable.ListBuffer
import scala.io.Source

object IdsGenerator extends App {

  //  val startId = 0
  val startId = 138
  val templateClass = classOf[BaseEntryIdFormatA].getSimpleName

  val path = Paths.get(s"${System.getProperty("user.dir")}/core/src/main/scala/swaydb/core/segment/format/a/entry/id/$templateClass.scala")

  val source = Source.fromFile(path.toString)

  try {
    val (lines, maxID) =
      source
        .getLines
        .foldLeft((ListBuffer.empty[String], startId)) {
          case ((lines, id), oldLine) =>
            //            if (oldLine.matches(""".*BaseEntryIdFormatA\(\d+\).*""") && oldLine.contains("Deadline.NoDeadline")) {
            if (oldLine.matches(""".*BaseEntryIdFormatA\(\d+\).*""") && !oldLine.contains("Deadline.NoDeadline")) {
              val nextLine = oldLine.replaceAll("""BaseEntryIdFormatA\(\d+\)""", s"""BaseEntryIdFormatA($id)""")
              (lines += nextLine, id + 1)
            } else {
              (lines += oldLine, id)
            }
        }

    val content = Slice.writeString(lines.mkString("\n"))
    Effect.replace(content, path).get
    println(s"maxID: ${maxID - 1}")
  } finally {
    source.close()
  }
}
