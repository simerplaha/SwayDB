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

package swaydb.core.segment.entry.generators

import swaydb.core.segment.entry.id._
import swaydb.slice.Slice
import swaydb.effect.Effect

import java.nio.file.Paths
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
      Effect.replace(content.toByteBufferWrap, path)
      source.close()
      println(s"maxID: ${maxID - 1}")
      maxID
  }
}
