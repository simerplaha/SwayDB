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
import scala.collection.mutable.ListBuffer
import scala.io.Source
import swaydb.core.io.file.IOEffect
import swaydb.core.segment.format.a.entry.id._
import swaydb.data.slice.Slice

object IdsGenerator extends App {

  val startId = 0
  val templateClass = classOf[BaseEntryIdFormatA].getSimpleName

  val path = Paths.get(s"${System.getProperty("user.dir")}/core/src/main/scala/swaydb/core/segment/format/a/entry/id/$templateClass.scala")

  val source = Source.fromFile(path.toString)

  try {
    val (lines, maxID) =
      source
        .getLines
        .foldLeft((ListBuffer.empty[String], startId)) {
          case ((lines, id), oldLine) =>
            if (oldLine.matches(""".*BaseEntryIdFormatA\(\d+\).*""")) {
              val nextLine = oldLine.replaceAll("""BaseEntryIdFormatA\(\d+\)""", s"""BaseEntryIdFormatA($id)""")
              (lines += nextLine, id + 1)
            } else {
              (lines += oldLine, id)
            }
        }

    val content = Slice.writeString(lines.mkString("\n"))
    IOEffect.replace(content, path).get
    println(s"maxID: ${maxID - 1}")
  } finally {
    source.close()
  }
}
