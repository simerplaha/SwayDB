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

package swaydb.core.segment.format.a.entry.generators

import java.nio.file.Paths

import swaydb.core.io.file.Effect
import swaydb.core.segment.format.a.entry.id._
import swaydb.data.slice.Slice

import scala.collection.mutable.ListBuffer
import scala.io.Source

object IdsGenerator extends App {

  val templateClass = classOf[BaseEntryIdFormatA].getSimpleName

  val path = Paths.get(s"${System.getProperty("user.dir")}/core/src/main/scala/swaydb/core/segment/format/a/entry/id/$templateClass.scala")

  val matches =
    Seq(
      (line: String) => line.matches(""".*BaseEntryIdFormatA\(\d+\).*""") && line.contains("Deadline.NoDeadline") && !line.contains("Compressed"),
      (line: String) => line.matches(""".*BaseEntryIdFormatA\(\d+\).*""") && line.contains("Deadline.Uncompressed"),
      (line: String) => line.matches(""".*BaseEntryIdFormatA\(\d+\).*""") && line.contains("Deadline.NoDeadline") && line.contains("Compressed"),
      (line: String) => line.matches(""".*BaseEntryIdFormatA\(\d+\).*""") && !line.contains("Deadline.NoDeadline") && !line.contains("Deadline.Uncompressed")
    )

  matches.foldLeft(0) {
    case (startId, lineMatcher) =>
      val source = Source.fromFile(path.toString)
      val (lines, maxID) =
        source
          .getLines
          .foldLeft((ListBuffer.empty[String], startId)) {
            case ((lines, id), oldLine) =>
              if (lineMatcher(oldLine)) {
                val nextLine = oldLine.replaceAll("""BaseEntryIdFormatA\(\d+\)""", s"""BaseEntryIdFormatA($id)""")
                (lines += nextLine, id + 1)
              } else {
                (lines += oldLine, id)
              }
          }

      val content = Slice.writeString[Byte](lines.mkString("\n"))
      Effect.replace(content, path)
      source.close()
      println(s"maxID: ${maxID - 1}")
      maxID
  }
}
