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
import java.nio.file.{Files, Paths}

import swaydb.core.segment.format.one.entry.id._

import scala.collection.JavaConverters._

/**
  * Generates if conditions for all readers.
  */
object ReaderConditionsGenerator extends App {
  implicit class Implicits(entryId: EntryId) {
    def name =
      entryId.getClass.getName.dropRight(1).replaceAll("swaydb.core.segment.format.one.entry.id.", "").replaceAll("\\$", ".")
  }

  def write(ids: List[EntryId]): Unit = {
    val className = ids.head.getClass.getName.replaceAll("swaydb.core.segment.format.one.entry.id.", "").split("\\$").head.replace("EntryId", "Reader")
    val targetIdClass = Paths.get(s"${System.getProperty("user.dir")}/core/src/main/scala/swaydb/core/segment/format/one/entry/reader/matchers/$className.scala")
    val allLines = Files.readAllLines(targetIdClass).asScala
    val writer = new PrintWriter(targetIdClass.toFile)
    val failure = """scala.util.Failure(new Exception(this.getClass.getSimpleName + " - Reader not implemented for id: " + id))"""

    val allNewConditions =
      ids.sortBy(_.id).grouped(20).zipWithIndex map {
        case (groupedIds, groupIndex) =>
          val innerIfBlock =
            groupedIds.zipWithIndex map {
              case (id, inGroupIndex) =>
                val typedId = id.name

                val targetFunction =
                  if (className.contains("Remove"))
                    s"reader($typedId, indexReader, indexOffset, nextIndexOffset, nextIndexSize, previous)"
                  else
                    s"reader($typedId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous)"

                val ifCondition = s"if (id == $typedId.id) $targetFunction"

                if (inGroupIndex == 0)
                  s"\t\t\t$ifCondition"
                else if (inGroupIndex == groupedIds.size - 1) {
                  s"""\t\t\telse $ifCondition
                     |\t\t\telse $failure""".stripMargin
                } else
                  s"\t\t\telse $ifCondition"
            } mkString "\n"

          val ifCondition = s"if (id >= ${groupedIds.head.name}.id && id <= ${groupedIds.last.name}.id)\n$innerIfBlock"

          if (groupIndex == 0)
            s"\t\t$ifCondition"
          else if (groupIndex == groupedIds.size - 1) {
            s"""s"\t\telse $ifCondition"
               |\t\telse $failure\n""".stripMargin
          }
          else
            s"\t\telse $ifCondition"
      }

    val lastElse =
      s"""\t\telse $failure"""

    val conditionStartIndex = allLines.zipWithIndex.find { case (line, index) => line.contains("//GENERATED") }.get._2
    val allNewLines = allLines.take(conditionStartIndex) ++ Seq("\t//GENERATED CONDITIONS") ++ allNewConditions ++ Seq(lastElse, "}")

    writer.write(allNewLines.mkString("\n"))
    writer.close()
  }

  def ids: Seq[List[EntryId]] =
    Seq(
      PutKeyUncompressedEntryId.keyIdsList,
      PutKeyFullyCompressedEntryId.keyIdsList,
      PutKeyPartiallyCompressedEntryId.keyIdsList,
      RemoveEntryId.keyIdsList,
      UpdateKeyUncompressedEntryId.keyIdsList,
      UpdateKeyFullyCompressedEntryId.keyIdsList,
      UpdateKeyPartiallyCompressedEntryId.keyIdsList,
      RangeKeyUncompressedEntryId.keyIdsList,
      RangeKeyFullyCompressedEntryId.keyIdsList,
      RangeKeyPartiallyCompressedEntryId.keyIdsList,
      GroupKeyUncompressedEntryId.keyIdsList,
      GroupKeyFullyCompressedEntryId.keyIdsList,
      GroupKeyPartiallyCompressedEntryId.keyIdsList
    )

  ids foreach write

}
