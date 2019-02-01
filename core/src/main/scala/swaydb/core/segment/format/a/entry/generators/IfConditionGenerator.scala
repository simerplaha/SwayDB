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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.segment.format.a.entry.generators

import java.io.PrintWriter
import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._
import swaydb.core.segment.format.a.entry.id.EntryId.Key
import swaydb.core.segment.format.a.entry.id._

/**
  * Generates if conditions for all readers.
  */
object IfConditionGenerator extends App {
  implicit class Implicits(entryId: EntryId) {
    def name =
      entryId.getClass.getName.dropRight(1).replaceAll("swaydb.core.segment.format.a.entry.id.", "").replaceAll("\\$", ".")
  }

  def write(fileNumber: Int, ids: List[EntryId]): Unit = {
//    val className = ids.head.getClass.getName.replaceAll("swaydb.core.segment.format.a.entry.id.", "").split("\\$").head.replace("EntryId", s"KeyEntryReader")
    val targetIdClass = Paths.get(s"${System.getProperty("user.dir")}/core/src/main/scala/swaydb/core/segment/format/a/entry/reader/matchers/BaseKeyReader$fileNumber.scala")
    val allLines = Files.readAllLines(targetIdClass).asScala
    val writer = new PrintWriter(targetIdClass.toFile)

    //    val failure = """scala.util.IO.Failure(new Exception(this.getClass.getSimpleName + " - Reader not implemented for id: " + id))"""
    val notFound = "None"

    val defaultGroupSize = 20

    val allNewConditions: Iterator[String] =
      ids.sortBy(_.id).grouped(defaultGroupSize).zipWithIndex map {
        case (groupedIds, groupIndex) =>
          val innerIfBlock =
            groupedIds.zipWithIndex map {
              case (id, inGroupIndex) =>
                val typedId = id.name

                val targetFunction =
                  s"Some(reader($typedId, indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous))"

                val ifCondition = s"if (id == $typedId.id) $targetFunction"

                if (inGroupIndex == 0)
                  s"\t\t\t$ifCondition"
                else if (inGroupIndex == groupedIds.size - 1) {
                  s"""\t\t\telse $ifCondition
                     |\t\t\telse $notFound""".stripMargin
                } else
                  s"\t\t\telse $ifCondition"
            } mkString "\n"

          val ifCondition = s"if (id >= ${groupedIds.head.name}.id && id <= ${groupedIds.last.name}.id)\n$innerIfBlock"

          if (groupIndex == 0)
            s"\t\t$ifCondition"
          else
            s"\t\telse $ifCondition"
      }

    val notFoundElse =
      s"""else $notFound"""

    val conditionStartIndex = allLines.zipWithIndex.find { case (line, index) => line.contains("//GENERATED") }.get._2
    val allNewLines =
      allLines.take(conditionStartIndex) ++
        Seq("\t//GENERATED CONDITIONS") ++
        Seq(s"\tif(id >= ${ids.head.name}.id && id <= ${ids.last.name}.id)") ++
        allNewConditions ++
        Seq("\t\t" + notFoundElse, "\t" + notFoundElse, "}")

    writer.write(allNewLines.mkString("\n"))
    writer.close()
  }

  def groupTime(key: Key): Int =
    key match {
      case _: EntryId.Time.NoTime =>
        1
      case _: EntryId.Time.FullyCompressed =>
        2
      case _: EntryId.Time.PartiallyCompressed =>
        3
      case _: EntryId.Time.Uncompressed =>
        4
    }

  Seq(
    BaseEntryId.keyIdsList
  ) foreach {
    keyIds =>
      val groups: Map[Int, List[EntryId]] =
        keyIds.groupBy {
          case key: Key.PartiallyCompressed => 1 + groupTime(key)
          case key: Key.Uncompressed => 6 + groupTime(key)
          case key: Key.FullyCompressed => 11 + groupTime(key)
        }

      groups foreach {
        case (keyCompressionType, ids) =>
          write(keyCompressionType, ids)
      }
  }

}
